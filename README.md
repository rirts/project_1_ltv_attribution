# Marketing Attribution & LTV Dashboard  
*(MySQL + Python + Power BI)*

This project integrates **MySQL databases**, **Python processing**, and **Power BI visualization** to analyze marketing campaign effectiveness through different **attribution models** and business metrics such as **LTV**, **CAC**, and **ROAS**.


## Project Structure
- `powerbi/` — final Power BI dashboard (`Dashboard_P1.pbix`) stored via Git LFS  
- `sql/` — SQL scripts to create and populate tables  
- `scripts/` — Python ETL scripts  
- `data/` — (optional) sample data  
- `requirements.txt` — Python dependencies  
- `.env.example` — example environment variables  
- `README.md` — project documentation 



## How to Run the Project

### 1. Set up Python environment
python -m venv .venv
.\.venv\Scripts\activate   # On Windows
pip install -r requirements.txt


### 2. Configure environment variables
Create a **.env** file based  on **env.example**:
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=demo_user
MYSQL_PASSWORD=demo_pass
MYSQL_DB=analytics_portfolio

### 3. Load database
Run the SQL scripts inside **sql/** to create tables and populate initial data.

### 4. Connect Power BI
Open **powerbi/Dashboard_P1.pbix**
Update connection credentials to your **MySQL** instance
Refresh data

## Key metrics
**Attributed Revenue (Selected)** - Revenue assigned to a channel based on chosen attribution model 
**Total Revenue (Selected)** - Total revenue from orders
**Orders (Selected)** - Unique orders
**Customers (Selected)** – Unique customers
**Total Spend (Selected)** – Total marketing spend
**CAC (Selected)** – Customer acquisition cost
**Avg LTV (sel)** – Average lifetime value, adjusted by horizon_days
**LTV/CAC (sel)** – Ratio between lifetime value and acquisition cost
**ROAS (Selected)** – Return on advertising spend

## Attribution Models Implemented
**First Click** – Assigns all value to the first customer touchpoint
**Last Click** – Assigns all value to the last touchpoint before purchase
**Linear** – Distributes value evenly across all touchpoints
**Time Decay** – Gives more weight to touchpoints closer to the purchase

## Filters and segmentation 
Date range
Attribution model
Marketing channel
Horizon Days

## Dashboard example
<img width="1182" height="657" alt="image" src="https://github.com/user-attachments/assets/f407b83e-cb62-4d80-8d6f-b6ebd03a58ac" />
<img width="1178" height="661" alt="image" src="https://github.com/user-attachments/assets/bd6a593c-f134-4447-8ba1-d7fba663b881" />
<img width="1175" height="656" alt="image" src="https://github.com/user-attachments/assets/1283ac03-1fea-4140-a93a-dc9e1a557d41" />

## Actionable Insights From The Dashboard
Identify which channels generate **customers with better LTV/CAC**
Detect campaigns producing **negative ROAS** that should be optimized  or discontinued
Compare how metrics change under **different attribution models** 

## Notes 
The .pbix file is stored using Git LFS due to its size.
The actual .env file is never uploaded; use .env.example as a template.
Sample data is fictional and does not represent real customer information.

## Credits
Project developed as part of a data analytics portfolio, integrating SQL, Python, and Power BI.




