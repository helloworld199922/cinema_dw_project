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
    agegroup = Column(String)  # 'Under 18', '18-40', '40+'

class DimDirector(Base):
    __tablename__ = 'dim_director'
    directorid = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class DimMovie(Base):
    __tablename__ = 'dim_movie'
    movieid = Column(Integer, primary_key=True)
    title = Column(String)
    genre = Column(String)
    releasedate = Column(Date)
    directorid = Column(Integer, ForeignKey('dim_director.directorid'))

class DimCinema(Base):
    __tablename__ = 'dim_cinema'
    cinemaid = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)

class DimHall(Base):
    __tablename__ = 'dim_hall'
    hallid = Column(Integer, primary_key=True)
    cinemaid = Column(Integer, ForeignKey('dim_cinema.cinemaid'))

class DimBrowser(Base):
    __tablename__ = 'dim_browser'
    browserid = Column(Integer, primary_key=True)
    browsername = Column(String)

class FactTicketSales(Base):
    __tablename__ = 'fact_ticketsales'
    transactionid = Column(Integer, primary_key=True)
    customerid = Column(Integer, ForeignKey('dim_customer.customerid'))
    movieid = Column(Integer, ForeignKey('dim_movie.movieid'))
    cinemaid = Column(Integer, ForeignKey('dim_cinema.cinemaid'))
    hallid = Column(Integer, ForeignKey('dim_hall.hallid'))
    dateid = Column(Date)
    totalprice = Column(Float)
    ticketcount = Column(Integer)
    browserid = Column(Integer, ForeignKey('dim_browser.browserid'))

# Reset and recreate the database
def reset_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

# Generate directors
def generate_directors():
    directors = [
        "Christopher Nolan", "Steven Spielberg", "Quentin Tarantino",
        "Martin Scorsese", "James Cameron", "Ridley Scott", "Tim Burton",
        "Francis Ford Coppola", "Alfred Hitchcock", "Stanley Kubrick"
    ]
    session.bulk_save_objects([DimDirector(name=d) for d in directors])
    session.commit()
    print(f"✅ Inserted {len(directors)} directors.")

# Generate customers
def generate_customers(n=50000):
    customers = []
    for i in range(n):
        dob = datetime.date(random.randint(1950, 2010), random.randint(1, 12), random.randint(1, 28))
        agegroup = 'Under 18' if dob.year > 2006 else '18-40' if dob.year > 1980 else '40+'
        customers.append(DimCustomer(
            customerid=i+1,
            name=f"Customer {i+1}",
            dob=dob,
            gender=random.choice(['M', 'F']),
            agegroup=agegroup
        ))
    session.bulk_save_objects(customers)
    session.commit()
    print(f"✅ Inserted {n} customers.")

# Generate movies
def generate_movies(n=500):
    genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror"]
    directors = session.query(DimDirector).all()
    session.bulk_save_objects([
        DimMovie(movieid=i+1, title=f"Movie {i+1}", genre=random.choice(genres),
                 releasedate=datetime.date(random.randint(2000, 2023), random.randint(1, 12), random.randint(1, 28)),
                 directorid=random.choice(directors).directorid)
        for i in range(n)
    ])
    session.commit()
    print(f"✅ Inserted {n} movies.")

# Generate cinemas
def generate_cinemas():
    cities = ["Doha", "Al-Khor", "Al-Wakrah", "Lusail"]
    session.bulk_save_objects([
        DimCinema(cinemaid=i+1, name=f"Cinema {i+1}", city=random.choice(cities))
        for i in range(1, 41)  # 10 per city
    ])
    session.commit()
    print(f"✅ Inserted 40 cinemas.")

# Generate halls
def generate_halls(n=200):
    cinemas = session.query(DimCinema).all()
    session.bulk_save_objects([
        DimHall(hallid=i+1, cinemaid=random.choice(cinemas).cinemaid) for i in range(n)
    ])
    session.commit()
    print(f"✅ Inserted {n} halls.")

# Generate browsers
def generate_browsers():
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    session.bulk_save_objects([DimBrowser(browserid=i+1, browsername=b) for i, b in enumerate(browsers)])
    session.commit()
    print(f"✅ Inserted {len(browsers)} browsers.")

# Generate ticket sales
def generate_ticket_sales(n=5000000):
    customers = [c.customerid for c in session.query(DimCustomer.customerid).all()]
    movies = [m.movieid for m in session.query(DimMovie.movieid).all()]
    cinemas = [c.cinemaid for c in session.query(DimCinema.cinemaid).all()]
    halls = [h.hallid for h in session.query(DimHall.hallid).all()]
    browsers = [b.browserid for b in session.query(DimBrowser.browserid).all()]

    transactions = []
    for i in range(n):
        transactions.append(FactTicketSales(
            transactionid=i+1,
            customerid=random.choice(customers),
            movieid=random.choice(movies),
            cinemaid=random.choice(cinemas),
            hallid=random.choice(halls),
            dateid=datetime.date(random.randint(2014, 2018), random.randint(1, 12), random.randint(1, 28)),
            totalprice=round(random.uniform(5, 30), 2),
            ticketcount=random.randint(1, 10),
            browserid=random.choice(browsers)
        ))
        if i % 100000 == 0 and i > 0:
            session.bulk_save_objects(transactions)
            session.commit()
            transactions = []
            print(f"✅ Inserted {i} ticket sales...")

    session.bulk_save_objects(transactions)
    session.commit()
    print(f"✅ Inserted {n} ticket sales.")

# Run the script
def main():
    reset_database()
    generate_directors()
    generate_customers()
    generate_movies()
    generate_cinemas()
    generate_halls()
    generate_browsers()
    generate_ticket_sales()
    print("✅ Database Population Completed Successfully!")

if __name__ == "__main__":
    main()
