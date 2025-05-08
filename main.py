from scraper_gh import DiscomDrawalScheduleScraper
from datetime import datetime


def main():
    scraper = DiscomDrawalScheduleScraper()

    # You might want to scrape in smaller chunks to avoid timeouts
    start_date = datetime(2025, 1, 1)  # Start date
    end_date = datetime(2025, 5, 7)  # End date (first month only for testing)

    try:
        scraper.scrape_date_range(start_date, end_date)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
