from src.core.config.environment import INTERNAL_MODE
from src.core.init import init
from src.ui.sidebar import sidebar
from src.internal.auth import log_in

init()

if INTERNAL_MODE:
    log_in()

sidebar().run()
