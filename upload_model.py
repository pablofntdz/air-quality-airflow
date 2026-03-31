from huggingface_hub import HfApi

api = HfApi()

api.create_repo(repo_id="pablofntdz/air-quality-scikit-learn", exist_ok=True)

api.upload_folder(
    folder_path="include/models",
    repo_id="pablofntdz/air-quality-scikit-learn"
)