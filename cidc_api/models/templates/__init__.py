from .assay_templates import *
from .manifest_templates import *

from .trial_metadata import *
from .file_metadata import *
from .assay_metadata import *

from .model_core import *
from .core import *
from .utils import *

TEMPLATE_MAP = {
    get_full_template_name(v): v
    for v in dict(globals(), **locals())
    if isinstance(v, MetadataTemplate)
}
