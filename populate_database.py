import pandas as pd
from sqlalchemy import create_engine

# Database connection string (Update 'your_user' and 'your_password' accordingly)
DATABASE_URL = "postgresql://your_user:your_password@localhost:5432/cinemadw"
engine = create_engine(DATABASE_URL)

# Sample Data for Dim_Customer
customers_df = pd.DataFrame({
    "customerid": range(1, 1001),
    "name": [f"Customer {i}" for i in range(1, 1001)],
    "dob": pd.date_range(start='1970-01-01', periods=1000, freq='M').strftime('%Y-%m-%d'),
    "gender": ["M" if i % 2 == 0 else "F" for i in range(1, 1001)],
    "agegroup": ["1-10" if i < 10 else "11-20" if i < 20 else "21-30" if i < 30 else "31+" for i in range(1, 1001)]
})

# Insert into Dim_Customer table
customers_df.to_sql("dim_customer", engine, if_exists="append", index=False)
print("✅ Inserted rows into Dim_Customer")

# Sample Data for fact_ticketsales
ticketsales_df = pd.DataFrame({
    "transactionid": range(1, 1001),
    "customerid": [i for i in range(1, 1001)],
    "movieid": [i % 10 + 1 for i in range(1, 1001)],
    "cinemaid": [i % 5 + 1 for i in range(1, 1001)],
    "hallid": [i % 3 + 1 for i in range(1, 1001)],
    "promotionid": [i % 2 + 1 for i in range(1, 1001)],
    "dateid": pd.date_range(start='2022-01-01', periods=1000, freq='D').strftime('%Y%m%d'),
    "browserid": [i % 4 + 1 for i in range(1, 1001)],
    "transactiontype": ["Online" if i % 2 == 0 else "Box Office" for i in range(1, 1001)],
    "showtime": [f"{i%24:02}:00:00" for i in range(1, 1001)],
    "totalprice": [i * 10.5 for i in range(1, 1001)],
    "ticketcount": [i % 5 + 1 for i in range(1, 1001)]
})

# Insert into fact_ticketsales table
ticketsales_df.to_sql("fact_ticketsales", engine, if_exists="append", index=False)
print("✅ Inserted rows into fact_ticketsales")
