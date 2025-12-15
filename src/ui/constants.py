from typing import Final
from src.core.types import ScalingMethod, Frequency

SCALING_LABELS: Final[dict[ScalingMethod, str]] = {
    ScalingMethod.NORMAL: "Z-Score",
    ScalingMethod.MINMAX: "Min/Max",
    ScalingMethod.RANK: "Rank",
}

FREQUENCY_LABELS: Final[dict[Frequency, str]] = {
    Frequency.WEEKLY: "Every week",
    Frequency.WEEKS2: "Every 2 weeks",
    Frequency.WEEKS4: "Every 4 weeks",
    Frequency.WEEKS8: "Every 8 weeks",
    Frequency.WEEKS13: "Every 13 weeks",
    Frequency.WEEKS26: "Every 26 weeks",
    Frequency.WEEKS52: "Every 52 weeks",
}
