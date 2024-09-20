import subprocess

def run_pipeline():
    #run script loading data using Etherscan API
    print("Starting data collection....")
    subprocess.run(["python3", "scripts/fetch_data.py"])
    
    #run script transforming and loading data to ProgeSQL database 
    print("Starting transform and load to database.....")
    subprocess.run(["python3", "scripts/clean_data.py"])
    
if __name__ == "__main__":
    run_pipeline()
