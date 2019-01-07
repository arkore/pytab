import struct
from collections import defaultdict
import re
import datetime


class MapInfoReader(object):
    codec_lookup = defaultdict(lambda: 'latin-1')
    type_conv = {'Char': str,
                 'Integer': int,
                 'Decimal': float}
    tab_dbf_type = {str: "C", int: "C", float: "N"}

    def __init__(self, filepath):
        self.__index = 0
        self.filepath = filepath

        self.version = None
        self.codec = None
        self.numfields = 0
        self.headers = []
        self.header_format = {}

        self._parse_tab()

        self._parse_dat()

        self._parse_id()

        self._parse_map()

        self.rows = self.records

    def __iter__(self):
        self.__index = 0
        return self

    def next(self):
        if self.__index >= len(self.records):
            raise StopIteration
        else:
            self.__index += 1
            return self.records[self.__index-1]

    def _parse_tab(self):
        tab_path = self.filepath  # TODO: Check file exists
        with open(tab_path, 'rb') as f:
            # TODO: Handle unexpected EOF
            # Assert MapInfo Tab Format
            first_line = f.readline()
            assert first_line.startswith(b"!table")

            # Get MapInfo Version Number
            version_line = f.readline()
            assert version_line.startswith(b"!version")
            version_re = re.match(r"^!version (\d+)\r\n", version_line.decode('utf-8'))
            if version_re:
                self.version = version_re.groups()[0]
            assert self.version

            # Text codec
            codec_line = f.readline()
            assert codec_line.startswith(b"!charset")
            codec_re = re.match(r'^!charset ([\w\d-]+)', codec_line.decode('utf-8'))
            if codec_re:
                self.codec = self.codec_lookup[codec_re.groups()[0].lower()]

            # Skip search blank lines
            line = f.readline()
            while not line.startswith(b"Definition Table"):
                line = f.readline()

            # Number of Fields
            # type_line = f.readline()  # TODO: handle type line
            _ = f.readline()

            fieldcount_line = f.readline()
            assert fieldcount_line.startswith(b"  Field")  # TODO: Check one field isn't listed as field
            fieldcount_re = re.match(r'[ ]+Fields (\d+)', fieldcount_line.decode('utf-8'))
            if fieldcount_re:
                self.numfields = int(fieldcount_re.groups()[0])

            for idx in range(self.numfields):
                # TODO: Header objects?
                field_line = list(filter(None, f.readline().decode('utf-8').split(' ')))
                assert ";" in field_line[-1]

                try:
                    field = field_line.pop(0)
                    self.headers.append(field)
                    self.header_format[field] = {}
                except IndexError:
                    self.headers.append(None)
                    print("Could not find field name")

                try:
                    field_type = field_line.pop(0)
                    assert field_type in self.type_conv.keys()
                    self.header_format[field]['type'] = self.type_conv[field_type]
                except IndexError:
                    print("Could not find field type")

                try:
                    if self.header_format[field]['type'] is str:
                        field_length = int(field_line.pop(0).strip("()"))
                        self.header_format[field]['length'] = field_length
                    elif self.header_format[field]['type'] is float:
                        field_length = int(field_line.pop(0).strip("(,"))
                        field_decimal = int(field_line.pop(0).strip(")"))
                        self.header_format[field]['length'] = field_length
                        self.header_format[field]['decimal'] = field_decimal
                    elif self.header_format[field]['type'] is int:
                        self.header_format[field]['length'] = 4
                except IndexError:
                    print("Length parsing failed")

                try:
                    is_index = field_line.pop(0)
                    if is_index is "Index":
                        field_index = int(field_line.pop(0))
                        self.header_format[field]['index'] = field_index
                except IndexError:
                    pass

            # TODO: Metadata

    def _parse_dat(self):
        dat_path = self.filepath.rsplit(".",1)[0] + ".dat"

        with open(dat_path, 'rb') as f:
            # MapInfo Uses dBase IV format
            # (see: "http://web.archive.org/web/20150323061445/http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm")
            # (see: "https://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET")
            # First 32 bytes define the header scope
            headers = f.read(32)
            header = struct.unpack('<BBBBLHH20x', headers)
            # TODO: Extract version number from first 3 bits
            # TODO: Incomplete Transaction Flag
            # TODO: Language Driver ID
            version, year, month, day, self.numrec, self.lenheader, self.lenrec = header
            # TODO: Last modified date to python datetime
            year += 1900
            self.last_modified = datetime.datetime(year=year, month=month, day=day)

            numfields = (self.lenheader - 32 - 1) // 32
            assert numfields == self.numfields
            for header in self.headers:
                field_data = f.read(32)
                field_tuple = struct.unpack('11sc4xBB14x', field_data)
                name, datatype, length, decimal = field_tuple
                assert header.startswith(name.strip(b"\x00").decode(self.codec))
                assert self.header_format[header]['length'] == length
                assert self.tab_dbf_type[self.header_format[header]['type']] == datatype.decode(self.codec)
                if self.header_format[header]['type'] is float:
                    assert self.header_format[header]['decimal'] == decimal

            terminator = f.read(1)
            assert terminator == b'\r'

            # All DBF headers match the TAB definition
            self.recfrmt = '<1s'  # All records start with a deleted record byte and are little-endian
            frmtlenrec = 1
            for header in self.headers:
                if self.header_format[header]['type'] is str:
                    self.recfrmt += '{}s'.format(self.header_format[header]['length'])
                    frmtlenrec += self.header_format[header]['length']
                elif self.header_format[header]['type'] is float:
                    self.recfrmt += '{}s'.format(self.header_format[header]['length'])
                    frmtlenrec += self.header_format[header]['length']
                elif self.header_format[header]['type'] is int:
                    self.recfrmt += 'I'
                    frmtlenrec += 4
            assert frmtlenrec == self.lenrec

            self.records = []
            for idx in range(self.numrec):
                rec_data = f.read(self.lenrec)
                rec_data = struct.unpack(self.recfrmt, rec_data)
                # Current records have " " while deleted have "*"
                # TODO: Handle deleted records
                if rec_data[0].decode(self.codec) == " ":
                    rec = dict.fromkeys(self.headers)
                    for col, header in enumerate(self.headers):
                        if self.header_format[header]['type'] is int:
                            rec[header] = rec_data[col + 1]
                        elif self.header_format[header]['type'] is str:
                            rec[header] = rec_data[col + 1].strip(b"\x00").decode(self.codec)
                        elif self.header_format[header]['type'] is float:
                            rec[header] = float(rec_data[col + 1])
                    self.records.append(rec)

    def _parse_id(self):
        # TODO: ID file can be optional so must handle missing file
        id_path = self.filepath.rsplit(".", 1)[0] + ".id"
        self.spatial_index = []
        with open(id_path, 'rb') as f:
            # Spatial Index is f.seek position for the .map file
            # If record has Spatial Index == 0 then no geometry
            for idx in range(self.numrec):
                id_join = f.read(4)  # Spatial Index is a 4 byte, 32 bit unsigned int
                (spatial_id,) = struct.unpack("<L", id_join)
                self.spatial_index.append(spatial_id)
            eof = f.read(1)
            assert len(eof) == 0

    def _parse_map(self):
        map_path = self.filepath.rsplit(".", 1)[0] + ".map"

        self.feature_geom = []
        with open(map_path, 'rb') as f:
            # Version 300 - 500 header is 512 bytes
            # Version 500+ header is 1024 bytes
            header_block = f.read(512)
            (magic,) = struct.unpack("<l", header_block[256:260])
            assert magic == 42424242
            (self.map_version,) = struct.unpack("<h", header_block[260:262])
            (self.map_block_size,) = struct.unpack("<h", header_block[262:264])
            (self.CoordSysToDistUnits,) = struct.unpack("<d", header_block[264:272])
            (mbrXmin, mbrYmin, mbrXmax, mbrYmax) = struct.unpack("<llll", header_block[272:288])
            (self.map_object_offset, ) = struct.unpack("<l", header_block[304:308])
            (self.map_deleted_offset, ) = struct.unpack("<l", header_block[308:312])
            (self.map_resource_offset, ) = struct.unpack("<l", header_block[312:316])
            (file_distance_units, ) = struct.unpack("<B", header_block[350:351])
            (index_type, CoordPrecision, CoordOriginCode, ReflectAxisCode) = struct.unpack("<BBBB", header_block[351:355])
            (projection_type, datum, coord_distance_units) = struct.unpack("<BBB", header_block[365:368])
            (self.x_scale, self.y_scale, self.x_offset, self.y_offset) = struct.unpack("<dddd", header_block[368:400])
            projection_params = struct.unpack("<dddddd", header_block[400:448])
            (datum_x, datum_y, datum_z) = struct.unpack("<ddd", header_block[448:472])

            # CoordOriginCode = Quadrant
            # 2,3,0 = -ve x
            # 3,4,0 = -ve y
            self.x_quad = 1.0
            self.y_quad = 1.0
            if CoordOriginCode == 3 or CoordOriginCode == 0:
                self.x_quad = -1.0
                self.y_quad = -1.0
            elif CoordOriginCode == 2:
                self.x_quad = -1.0
            elif CoordOriginCode == 4:
                self.y_quad = -1.0

            # Collect all blocks of type 2 (ODB's)
            f.seek(0)
            block = f.read(self.map_block_size)
            idx = 0
            index_blocks = []
            while len(block) == self.map_block_size:
                (block_type_byte,) = struct.unpack("<B", block[0:1])
                if block_type_byte == 2:
                    index_blocks.append(idx)
                idx += 1
                block = f.read(self.map_block_size)

            # Direct seek each geom for feature
            for row_id, idx in enumerate(self.spatial_index):
                # TODO: Check RowID for deletion marker
                f.seek(idx)
                (block_type,) = struct.unpack("<B", f.read(1))
                if block_type == 1:
                    # Short Point is ODB type 1
                    # Stores co-ordinates as a pair of 2 byte offset to a pair of 4 byte origin stored in the ODB header
                    # Must retrieve both the object itself and its ODB header to build geometry
                    block = f.read(9)
                    row_number, x, y, symb = struct.unpack("<lhhB", block)
                    assert row_number == row_id + 1

                    odbi_start = idx//self.map_block_size
                    assert odbi_start in index_blocks
                    f.seek(odbi_start*self.map_block_size)
                    header = f.read(12)
                    odbi, link, usedbytes, base_x, base_y = struct.unpack("<BBhll", header)
                    assert odbi == 2
                    # TODO: Update to a real geom object of some type
                    self.feature_geom.append({'x': self.x_quad * (x + base_x + self.x_offset)/self.x_scale,
                                              'y': self.y_quad * (y + base_y + self.y_offset)/self.y_scale,
                                              'symb': symb})

                else:
                    self.feature_geom.append(None)

    def _parse_ind(self):
        raise NotImplemented

if __name__ == '__main__':
    from configparser import ConfigParser
    test_config = ConfigParser()
    test_config.read("test.ini")
    path = test_config.get("mapinfo", "path", fallback="test.tab")
    reader = MapInfoReader(path)
