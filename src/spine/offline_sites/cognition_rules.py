from spine.offline_sites.cognition_runtime_config import CONFIG


FORBIDDEN_PHRASES = [
    "Primary cognition lens",
    "Layout implication",
    "review whether chart space is necessary",
    "should be interpreted through",
    "review alongside spread direction",
]


def clean_output(lines):
    cleaned = []

    for line in lines:
        skip = False

        for phrase in FORBIDDEN_PHRASES:
            if phrase.lower() in line.lower():
                skip = True
                break

        if not skip:
            cleaned.append(line.strip())

    return cleaned


def reserved_space_state(level):
    return CONFIG.reserved_space_states.get(level, "-")


def sigma_state(z):
    if z is None:
        return "contained"

    if abs(z) >= 2:
        return "high"

    if abs(z) >= 1:
        return "moderate"

    return "contained"


def compress(lines, max_lines):
    return lines[:max_lines]
