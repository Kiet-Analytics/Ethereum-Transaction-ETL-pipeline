# Ethereum Transaction Data Pipeline
This project automates the daily collection, processing, and storage of Ethereum transaction data using **Python** and **PostgreSQL**. It leverages the **Etherscan API** to retrieve Ethereum blockchain transaction details on a daily basis and organizes the data for future analysis. The pipeline is designed to run automatically every day, ensuring consistent data capture and storage.

# Features:
- **Daily Data Collection**: Automatically fetches Ethereum transaction data each day using the Etherscan API.
- **Data Cleaning**: Processes and cleans the raw transaction data using pandas for accuracy and consistency.
- **Database Storage**: Stores the cleaned data in a PostgreSQL database for efficient querying and analysis.
- **Automation**: The pipeline is scheduled to run daily, reducing manual effort and ensuring up-to-date daily data.

# Tech Stack:
- **Python**: Used for scripting and automation.
- **pandas**: For data cleaning and transformation.
- **PostgreSQL**: Database used to store transaction data.
- **SQLAlchemy**: For interaction between Python and the PostgreSQL database.
- **Etherscan API**: For fetching Ethereum transaction data.
