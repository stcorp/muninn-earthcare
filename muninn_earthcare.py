import os
import re
import zipfile
from datetime import datetime
import xml.etree.ElementTree as ET
from typing import Optional, Callable

from muninn.geometry import LineString, Point
from muninn.schema import Mapping, Text, Integer, Boolean, Timestamp
from muninn.util import copy_path
from muninn import Struct
from muninn import Error as MuninnError


# Namespaces

class EarthCARENamespace(Mapping):
    file_class = Text(index=True, optional=True)
    baseline = Text(index=True, optional=True)  # last two characters of the file class
    orbit_number = Integer(index=True, optional=True)
    frame_id = Text(index=True, optional=True)  # frame within an orbit: A, B, C, ...
    processing_center = Text(index=True, optional=True)
    processor_name = Text(index=True, optional=True)
    processor_version = Text(index=True, optional=True)
    version_number = Integer(index=True, optional=True)


def namespaces():
    return ["earthcare"]


def namespace(namespace_name):
    return EarthCARENamespace


# Product types

L0_PRODUCT_TYPES = [
    "ATL_NOM_0_",  # ATLID level 0 product (L0)
    "BBR_NOM_0_",  # BBR level 0 product (L0)
    "CPR_NOM_0_",  # CPR level 0 product (L0)
    "MSI_NOM_0_",  # MSI level 0 product (L0)
    "TLM_NOM_0_",  # GPS & STR annotated telemetry packets
]

L1_PRODUCT_TYPES = [
    "ATL_NOM_1B",  # ATLID nominal L1 product (ATLID L1)
    "ATL_DCC_1B",  # ATLID dark signal calibration product (ATLID L1)
    "ATL_CSC_1B",  # ATLID coarse spectral calibration product (ATLID L1)
    "ATL_FSC_1B",  # ATLID fine spectral calibration product (ATLID L1)
    "BBR_NOM_1B",  # BBR nominal L1 product (BBR L1)
    "BBR_SNG_1B",  # BBR single pixel L1 product (BBR L1)
    "BBR_SOL_1B",  # BBR solar calibration product (BBR L1)
    "BBR_LIN_1B",  # BBR linearity calibration product (BBR L1)
    "CPR_NOM_1B",  # CPR nominal L1 product (JAXA)
    "MSI_NOM_1B",  # MSI nominal L1 product (MSI L1)
    "MSI_BBS_1B",  # MSI black body/deep space calibration product (MSI L1)
    "MSI_SD1_1B",  # MSI nominal solar diffuser calibration product (MSI L1)
    "MSI_SD2_1B",  # MSI redundant solar diffuser calibration product (MSI L1)
    "MSI_DRK_1B",  # MSI VNS dark signal calibration product (MSI L1)
    "MSI_TRF_1B",  # MSI TIR sensitivity reference parameters calibration product (MSI L1)
    "MSI_RGR_1C",  # MSI regridded L1 product (MSI L1)
    "AUX_JSG_1D",  # Joint standard grid (X-JSG)
    "AUX_MET_1D",  # ECMWF meteorological parameters on EarthCARE swath (X-MET)
]

L2_PRODUCT_TYPES = [
    "ATL_FM__2A",  # A-FM: ATLID feature mask (A-FM)
    "ATL_ALD_2A",  # A-ALD: ATLID aerosol layer descriptors (A-LAY)
    "ATL_CTH_2A",  # A-CTH: ATLID cloud top height (A-LAY)
    "ATL_AER_2A",  # A-AER: ATLID aerosol parameters (A-PRO)
    "ATL_EBD_2A",  # A-EBD: ATLID extinction, backscatter and depolarisation (A-PRO)
    "ATL_ICE_2A",  # A-ICE: ATLID ice parameters (A-PRO)
    "ATL_TC__2A",  # A-TC: ATLID target classification (A-PRO)
    "CPR_APC_2A",  # C-APC: CPR antenna pointing correction (C-APC)
    "CPR_CD__2A",  # C-CD: CPR Cloud Doppler parameters (C-PRO)
    "CPR_CLD_2A",  # C-CLD: CPR cloud parameters (C-CLD)
    "CPR_FMR_2A",  # C-FMR: CPR feature mask and radar reflectivity (C-PRO)
    "CPR_TC__2A",  # C-TC: CPR target classification (C-PRO)
    "MSI_AOT_2A",  # M-AOT: MSI aerosol optical thickness (M-AOT)
    "MSI_CM__2A",  # M-CM: MSI cloud mask (M-CLD)
    "MSI_COP_2A",  # M-COP: MSI cloud optical properties (M-CLD)
    "AC__TC__2B",  # AC-TC: ATLID/CPR target classification (AC-TC)
    "ACM_CAP_2B",  # ACM-CAP: Cloud and aerosol properties from ATLID/CPR/MSI (ACM-CAP)
    "ACM_COM_2B",  # ACM-COM: ATLID/CPR/MSI composite product (ACM-COM)
    "ACM_RT__2B",  # ACM-RT: Broadband radiances and fluxes from ATLID/CPR/MSI (ACM-COM)
    "ALL_3D__2B",  # ACMB-3D: 3D scene construction (ACMB-3D)
    "ALL_DF__2B",  # ACMB-DF: Radiative closure assessment, comparing broadband radiances and fluxes (ACMB-DF)
    "AM__ACD_2B",  # AM-ACD: ATLID/MSI aerosol column descriptor (AM-COL)
    "AM__CTH_2B",  # AM-CTH: ATLID/MSI cloud top height (AM-COL)
    "AM__MO__2B",  # AM-MO: ATLID/MSI merged observations (AM-MO)
    "BM__RAD_2B",  # BM-RAD: BBR radiances (using MSI for corrections) (BM-RAD)
    "BMA_FLX_2B",  # BMA-FLX: BBR fluxes (using MSI and ATLID for corrections) (BMA-FLX)
]

GEO_PRODUCT_TYPES = [
    "GEO_ATTOBS",  # Reconstructed Attitude File
    "GEO_ORBOBS",  # Reconstructed Orbit File
]

FOS_PRODUCT_TYPES = [
    "AUX_ORBPRE",  # Predicted Orbit File
    "AUX_ORBRES",  # Reconstituted Orbit File
]

MPL_PRODUCT_TYPES = [
    "MPL_ORBSCT",  # Orbit Scenario File
]

def compress(paths, target_filepath, compresslevel=None):
    if compresslevel is None:
        compression = zipfile.ZIP_STORED
    else:
        compression = zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(target_filepath, "x", compression, compresslevel=compresslevel, strict_timestamps=False) \
            as archive:
        for path in paths:
            rootlen = len(os.path.dirname(path)) + 1
            if os.path.isdir(path):
                for base, dirs, files in os.walk(path):
                    for file in files:
                        fn = os.path.join(base, file)
                        archive.write(fn, fn[rootlen:])
            else:
                archive.write(path, path[rootlen:])


class EOFProduct(object):
    # filename_base_pattern is the pattern for the filename excluding any extension (and without trailing $)
    # extension can be None (for multifile products), or set to a specific extension (e.g. ".EOF")
    def __init__(self, product_type: str, filename_base_pattern: str = None, extension: str = None,
                 zipped: Optional[bool] = None):
        self.is_multi_file_product = extension is None
        self.product_type = product_type
        self.extension = extension
        self.filename_pattern = filename_base_pattern
        self.use_enclosing_directory = self.is_multi_file_product and not zipped
        if zipped is None:  # "None" means flexible zip handling
            if extension is None:
                self.filename_pattern += r"(\.ZIP$)?"
            else:
                self.filename_pattern += r"(%s|\.ZIP)$" % re.escape(extension)
        elif zipped:
            self.filename_pattern += r"\.ZIP$"
        else:
            if self.extension is not None:
                self.filename_pattern += r"%s$" % re.escape(extension)

    def enclosing_directory(self, properties):
        return properties.core.product_name

    @property
    def namespaces(self):
        return ["earthcare"]

    @staticmethod
    def archive_path(attributes):
        return os.path.join(
            attributes.core.product_type,
            attributes.core.validity_start.strftime("%Y"),
            attributes.core.validity_start.strftime("%m"),
            attributes.core.validity_start.strftime("%d"),
        )

    def is_zipped(self, filepath):
        return filepath.endswith(".ZIP")

    def parse_filename(self, filename):
        match = re.match(self.filename_pattern, os.path.basename(filename))
        if match:
            return match.groupdict()
        return None

    def identify(self, paths):
        if self.is_multi_file_product and not self.is_zipped(paths[0]):
            if len(paths) != 2:
                return False
            for path in paths:
                if os.path.isdir(path):
                    return False
                if re.match(self.filename_pattern, os.path.basename(path)) is None:
                    return False
            return True
        elif len(paths) != 1:
            return False
        if os.path.isdir(paths[0]):
            return False
        return re.match(self.filename_pattern, os.path.basename(paths[0])) is not None

    def analyze(self, paths, filename_only=False):
        if len(paths) > 1:
            file_path = os.path.splitext(paths[0])[0] + ".HDR"
        else:
            file_path = paths[0]
        file_name = os.path.basename(file_path)
        file_name_attrs = self.parse_filename(file_name)

        properties = Struct()
        core = properties.core = Struct()
        earthcare = properties.earthcare = Struct()

        core.product_name = os.path.splitext(file_name)[0]
        core.validity_start = datetime.strptime(file_name_attrs["validity_start"], "%Y%m%dT%H%M%SZ")
        if "validity_stop" in file_name_attrs:
            if file_name_attrs["validity_stop"] == "99999999T999999Z":
                core.validity_stop = datetime.max
            else:
                core.validity_stop = datetime.strptime(file_name_attrs["validity_stop"], "%Y%m%dT%H%M%SZ")

        if "file_class" in file_name_attrs:
            earthcare.file_class = file_name_attrs["file_class"]
            earthcare.baseline = earthcare.file_class[2:]
        if "version" in file_name_attrs:
            earthcare.version_number = int(file_name_attrs["version"])
        if "orbit_number" in file_name_attrs:
            earthcare.orbit_number = int(file_name_attrs["orbit_number"])
        if "frame_id" in file_name_attrs:
            earthcare.frame_id = file_name_attrs["frame_id"]

        if not filename_only:
            # Use header file to extract info
            if len(paths) == 1 and self.is_zipped(file_path):
                component_path = os.path.splitext(os.path.basename(file_path))[0] + ".HDR"
            else:
                component_path = None
            self._analyze_eof_header(self.read_xml_component(file_path, component_path), properties)

        return properties

    def read_xml_component(self, filepath, component_path=None):
        # filepath: Path given as input to the analyze() function
        # component_path: Path of the specific component to be read.

        # Open XML file (zipped or not) and return root element
        if self.is_zipped(filepath):
            if component_path is None:
                component_path = os.path.splitext(os.path.basename(filepath))[0]
                if self.extension is not None:
                    component_path += self.extension
            else:
                if not self.is_multi_file_product:
                    component_path = os.path.join(os.path.splitext(os.path.basename(filepath))[0], component_path)
            with zipfile.ZipFile(filepath) as zproduct:
                with zproduct.open(component_path) as file:
                    return ET.parse(file).getroot()
        else:
            if component_path is not None:
                filepath = os.path.join(filepath, component_path)
            with open(filepath) as file:
                return ET.parse(file).getroot()

    def _analyze_eof_header(self, root, properties):
        core = properties.core
        earthcare = properties.earthcare

        # Differentiate between Earth_Observation_Header and Earth_Explorer_Header
        if "Earth_Explorer_File" in root.tag:
            start_node_path = "./Earth_Explorer_Header/"
        else:
            # Case of a HDR file which starts directly by a "Earth_Explorer_Header" element
            start_node_path = "./"

        path = start_node_path + "Fixed_Header/Validity_Period/Validity_Start"
        core.validity_start = datetime.strptime(root.find(path).text, "UTC=%Y-%m-%dT%H:%M:%S")
        value = root.find(start_node_path + "Fixed_Header/Validity_Period/Validity_Stop").text
        if value == "UTC=9999-99-99T99:99:99":
            core.validity_stop = datetime.max
        else:
            core.validity_stop = datetime.strptime(value, "UTC=%Y-%m-%dT%H:%M:%S")
        path = start_node_path + "Fixed_Header/Source/Creation_Date"
        core.creation_date = datetime.strptime(root.find(path).text, "UTC=%Y-%m-%dT%H:%M:%S")
        path = start_node_path + "Fixed_Header/Source/System"
        earthcare.processing_center = root.find(path).text
        path = start_node_path + "Fixed_Header/Source/Creator"
        earthcare.processor_name = root.find(path).text
        path = start_node_path + "Fixed_Header/Source/Creator_Version"
        earthcare.processor_version = root.find(path).text

        # Extract geolocation information from frame start/stop position, if available
        path = start_node_path + "Variable_Header/MainProductHeader/frameStartCoordinates"
        coord = root.find(path)
        if coord is not None:
            start_lat = float(coord.find("./GeographicCoordinates/geographicLatitude").text)
            start_lon = float(coord.find("./GeographicCoordinates/geographicLongitude").text)
            coord = root.find(start_node_path + "Variable_Header/MainProductHeader/frameStopCoordinates")
            stop_lat = float(coord.find("./GeographicCoordinates/geographicLatitude").text)
            stop_lon = float(coord.find("./GeographicCoordinates/geographicLongitude").text)
            core.footprint = LineString([Point(start_lon, start_lat), Point(stop_lon, stop_lat)])

    def export_zip(self, archive, properties, target_path, paths):
        if self.is_zipped(paths[0]):
            assert len(paths) == 1, "zipped product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name)
        if self.extension:
            target_filepath = target_filepath[:-len(self.extension)]
        target_filepath += ".ZIP"
        compress(paths, target_filepath, compresslevel=1)
        return target_filepath


class EarthCAREProduct(EOFProduct):
    def __init__(self, product_type, extension=None, zipped=None):
        pattern = [
            r"^ECA",
            r"(?P<file_class>\w{4})",
            product_type,
            r"(?P<validity_start>\d{8}T\d{6}Z)",
            r"(?P<creation_date>\d{8}T\d{6}Z)",
            r"(?P<orbit_number>\d{5})(?P<frame_id>[A-Z])",
        ]
        super().__init__(product_type, filename_base_pattern=r"_".join(pattern), extension=extension, zipped=zipped)


class AUXProduct(EOFProduct):
    def __init__(self, product_type, zipped=None):
        pattern = [
            r"^ECA",
            r"(?P<file_class>\w{4})",
            product_type,
            r"(?P<validity_start>\d{8}T\d{6}Z)",
            r"(?P<validity_stop>\d{8}T\d{6}Z)",
            r"(?P<version>\d{4})",
        ]
        super().__init__(product_type, filename_base_pattern=r"_".join(pattern), extension=".EOF", zipped=zipped)


_product_types = dict(
    [(product_type, EarthCAREProduct(product_type)) for product_type in L0_PRODUCT_TYPES] +
    [(product_type, EarthCAREProduct(product_type)) for product_type in L1_PRODUCT_TYPES] +
    [(product_type, EarthCAREProduct(product_type)) for product_type in L2_PRODUCT_TYPES] +
    [(product_type, EarthCAREProduct(product_type, extension=".EOF")) for product_type in GEO_PRODUCT_TYPES] +
    [(product_type, AUXProduct(product_type)) for product_type in FOS_PRODUCT_TYPES] +
    [(product_type, AUXProduct(product_type)) for product_type in MPL_PRODUCT_TYPES]
)


def product_types():
    return _product_types.keys()


def product_type_plugin(product_type):
    return _product_types.get(product_type)
