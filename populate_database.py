import random
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

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

class DimDirector(Base):
    __tablename__ = 'dim_director'
    directorid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

class DimMovie(Base):
    __tablename__ = 'dim_movie'
    movieid = Column(Integer, primary_key=True)
    title = Column(String)
    genre = Column(String)
    releasedate = Column(Date)
    directorid = Column(Integer, ForeignKey('dim_director.directorid'))
    director = relationship("DimDirector", backref="movies")

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
    with engine.connect() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE;"))
        connection.execute(text("CREATE SCHEMA public;"))
        connection.commit()
    
    Base.metadata.create_all(engine)

# Generate synthetic data
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
    
    session.bulk_save_objects(customers)
    session.commit()
    print(f"✅ Inserted {n} customers.")

def generate_directors():
    directors = [
        "Christopher Nolan", "Steven Spielberg", "Quentin Tarantino",
        "Martin Scorsese", "James Cameron", "Ridley Scott", "Peter Jackson"
    ]
    director_objects = [DimDirector(name=name) for name in directors]
    session.bulk_save_objects(director_objects)
    session.commit()
    print(f"✅ Inserted {len(directors)} directors.")

def generate_movies(n=100):
    genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror']
    directors = session.query(DimDirector.directorid).all()
    
    if not directors:
        print("❌ Error: No directors found!")
        return

    movies = []
    for i in range(n):
        movies.append(DimMovie(
            movieid=i+1,
            title=f'Movie {i+1}',
            genre=random.choice(genres),
            releasedate=datetime.date(random.randint(2000, 2023), random.randint(1, 12), random.randint(1, 28)),
            directorid=random.choice(directors)[0]
        ))

    session.bulk_save_objects(movies)
    session.commit()
    print(f"✅ Inserted {n} movies.")

def generate_ticket_sales(n=1000000):
    transactions = []
    customers = session.query(DimCustomer.customerid).all()
    movies = session.query(DimMovie.movieid).all()

    if not customers or not movies:
        print("❌ Error: No customers or movies found!")
        return

    for i in range(n):
        transactions.append(FactTicketSales(
            transactionid=i+1,
            customerid=random.choice(customers)[0],
            movieid=random.choice(movies)[0],
            dateid=datetime.date(random.randint(2014, 2024), random.randint(1, 12), random.randint(1, 28)),
            totalprice=round(random.uniform(5, 30), 2),
            ticketcount=random.randint(1, 5)
        ))

        if i % 100000 == 0 and i > 0:  # Commit in batches for performance
            session.bulk_save_objects(transactions)
            session.commit()
            transactions = []
            print(f"✅ Inserted {i} ticket sales...")

    session.bulk_save_objects(transactions)
    session.commit()
    print(f"✅ Inserted {n} ticket sales.")

# Run the script
def main():
    print("🔄 Resetting Database...")
    reset_database()

    print("🎬 Generating Directors...")
    generate_directors()

    print("👥 Generating Customers...")
    generate_customers()

    print("🎞️ Generating Movies...")
    generate_movies()

    print("🎟️ Generating Ticket Sales...")
    generate_ticket_sales()

    print("✅ Database Population Completed Successfully!")

if __name__ == "__main__":
    main()
