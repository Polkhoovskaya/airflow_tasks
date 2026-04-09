from advanced_training_tasks.config import BASE_DIR
import os

def build_path(*parts):
    return os.path.join(BASE_DIR, *parts)