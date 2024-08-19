# Nasdaq-Vwap-Project

To calculate the **Volume Weighted Average Price (VWAP)** of each stock for all trading hours using NASDAQ ITCH 5.0 tick data. Coded in Python, just pass the updated `itch.gz` file in the code and simply run it.

**Instructions for Running the VWAP Calculation**

```bash
1. Clone the Repository:
Clone the repository to your local machine using the following command:
git clone <repository-url>

2. Add the ITCH File:
Place your ITCH data file into the project directory.

3. Run the VWAP Calculation:
Navigate to the directory containing the vwap.py file.
Execute the script using Python:
python vwap.py

The output will be saved in a file named vwap__output.txt. The results are sorted hourly.
To view results for different times, scroll through the vwap__output.txt file.
