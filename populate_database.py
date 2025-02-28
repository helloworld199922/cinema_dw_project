import random
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.orm import declarative_base, sessionmaker

# Database connection
DATABASE_URL = "postgresql://amnaalobaidli@localhost/cinemadw"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# Define tables
class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    customerid = Column(Integer, primary_key=True)
    name = Column(String)
    dob = Column(Date)
    gender = Column(String)
    agegroup = Column(String)

class DimMovie(Base):
    __tablename__ = 'dim_movie'
    movieid = Column(Integer, primary_key=True)
    title = Column(String)
    genre = Column(String)
    releasedate = Column(Date)

class FactTicketSales(Base):
    __tablename__ = 'fact_ticketsales'
    transactionid = Column(Integer, primary_key=True)
    customerid = Column(Integer, ForeignKey('dim_customer.customerid'))
    movieid = Column(Integer, ForeignKey('dim_movie.movieid'))
    dateid = Column(Date)
    totalprice = Column(Float)
    ticketcount = Column(Integer)

# Reset and recreate the database
def reset_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

# Generate customers
def generate_customers(n=5000):
    customers = []
    for i in range(n):
        dob = datetime.date(random.randint(1950, 2010), random.randint(1, 12), random.randint(1, 28))
        agegroup = 'Under 18' if dob.year > 2006 else '18-40' if dob.year > 1980 else '40+'
        customers.append(DimCustomer(
            customerid=i+1,
            name=f'Customer {i+1}',
            dob=dob,
            gender=random.choice(['M', 'F']),
            agegroup=agegroup
        ))

    session.bulk_save_objects(customers)  # ✅ Fixed Indentation
    session.commit()
    print(f"✅ Inserted {n} customers.")
    
# Generate movies
def generate_movies(n=100):   
    genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror']
    movies = []
    for i in range(n):
        movies.append(DimMovie(
            movieid=i+1,
            title=f'Movie {i+1}',
            genre=random.choice(genres),
            releasedate=datetime.date(random.randint(2000, 2023), random.randint(1, 12), random.randint(1, 28))
        ))
    session.bulk_save_objects(movies)
    session.commit()
    print(f"✅ Inserted {n} movies.")

# Generate ticket sales
def generate_ticket_sales(n=1000000):
    transactions = []
    customers = session.query(DimCustomer.customerid).all()
    movies = session.query(DimMovie.movieid).all()

    if not customers or not movies:
        print("❌ Error: No customers or movies found!")
        return  # ✅ Fixed Indentation
    
    customers = [c[0] for c in customers]
    movies = [m[0] for m in movies]
    
    for i in range(n):
        transactions.append(FactTicketSales(
            transactionid=i+1,
            customerid=random.choice(customers),
            movieid=random.choice(movies),
            dateid=datetime.date(random.randint(2014, 2024), random.randint(1, 12), random.randint(1, 28)),
            totalprice=round(random.uniform(5, 30), 2),
            ticketcount=random.randint(1, 5)
        ))  
        
        if i % 100000 == 0:  # Commit in batches
            session.bulk_save_objects(transactions)
            session.commit()
            transactions = []
            print(f"✅ Inserted {i} ticket sales...")
    
    session.bulk_save_objects(transactions)
    session.commit()
    print(f"✅ Inserted {n} ticket sales.")

# Run the script
def main():
    print("♻ Resetting Database...")  # ✅ Fixed Indentation
    reset_database()
    
    print("📌 Generating Customers...")
    generate_customers()
    
    print("📌 Generating Movies...")
    generate_movies()
            
    print("📌 Generating Ticket Sales...")
    generate_ticket_sales()
            
    print("✅ Database Population Completed Successfully!")
            
if __name__ == "__main__":
    main()
