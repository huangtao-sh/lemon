# 项目：lemon
# 作者：黄涛
# 邮件：hunto@163.com
# 创建：2017-07-22

from .document import config, P, Document, FileImported, Descriptor
from .cachedquery import load_query
from .__version__ import version as __version__
from .shadow import Shadow, shadow

shadow = Shadow

__all__ = 'config', 'P', 'Document', 'load_query', '__version__', 'Descriptor', 'shadow'
