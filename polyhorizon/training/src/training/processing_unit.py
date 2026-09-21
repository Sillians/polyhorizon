import torch

def get_accelerator():
    """
    Select best available accelerator:
    - MPS (Apple Silicon)
    - CUDA GPU
    - CPU fallback

    Returns:
        dict: {"accelerator": str, "devices": int}
    """
    if torch.backends.mps.is_available():
        return {"accelerator": "mps", "devices": 1}
    elif torch.cuda.is_available():
        return {"accelerator": "gpu", "devices": 1}
    else:
        return {"accelerator": "cpu", "devices": 1}
