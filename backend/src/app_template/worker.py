from app_template.settings import get_settings


def main() -> None:
    settings = get_settings()
    print(f"worker backend={settings.jobs_backend}")


if __name__ == "__main__":
    main()
