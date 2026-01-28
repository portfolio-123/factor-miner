from src.core.auth import login
from src.core.init import init
from src.ui.sidebar import sidebar

init()
login()
sidebar().run()
