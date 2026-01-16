from typing import Final
from src.core.types import ScalingMethod

SCALING_LABELS: Final[dict[ScalingMethod, str]] = {
    ScalingMethod.NORMAL: "Z-Score",
    ScalingMethod.MINMAX: "Min/Max",
    ScalingMethod.RANK: "Rank",
}


frequency_map = {
    1: "Weekly",
    7: "2 weeks",
    2: "4 weeks",
    8: "8 weeks",
    3: "13 weeks",
    9: "26 weeks",
    10: "52 weeks",
}

ANALYSIS_STATUS_COLORS: Final[dict[str, tuple[str, str]]] = {
    "completed": ("#e6f4ea", "#1e8e3e"),
    "running": ("#fff0b3", "#b06000"),
    "pending": ("#fff0b3", "#b06000"),
}
ANALYSIS_STATUS_COLORS_DEFAULT: Final[tuple[str, str]] = ("#fce8e6", "#c5221f")
