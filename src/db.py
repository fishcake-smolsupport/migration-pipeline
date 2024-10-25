from sqlalchemy import create_engine, MetaData, Table, exc, text

class DatabaseConnector:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        
    def setup_engine(self):
        try:
            self.engine = create_engine(self.database_url)
        except exc.SQLAlchemyError:
            print(f"Error: SQLAlchemy connection failed.")

        return self.engine
           
    def execute_query(self, statement: str) -> list:
        if not self.engine:
            print("Error: No engine established.")
            return []

        try:
            with self.engine.connect() as connection:
                results = connection.execute(text(statement))
                
            return results
        except exc.SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            return None

# class Table:
#     def __init__(self, engine, table_schema: str, table_name: str):
#         engine =  DatabaseConnector(db_url)
#         metadata = MetaData()

#         table = Table(
#                         table_name, 
#                         metadata, 
#                         schema = table_schema,
#                         autoload_with = engine
#                     )

#         return table

# def get_some_cars(engine, metadata): 
#   session = begin_session(engine)  

#   Cars   = metadata.tables['Cars']
#   Makes  = metadata.tables['CarManufacturers']

#   cars_cols = [ getattr(Cars.c, each_one) for each_one in [
#       'car_id',                   
#       'car_selling_status',       
#       'car_purchased_date', 
#       'car_purchase_price_car']] + [
#       Makes.c.car_manufacturer_name]

#   statuses = {
#       'selling'  : ['AVAILABLE','RESERVED'], 
#       'physical' : ['ATOURLOCATION'] }

#   inventory_conditions = alq.and_( 
#       Cars.c.purchase_channel == "Inspection", 
#       Cars.c.car_selling_status.in_( statuses['selling' ]),
#       Cars.c.car_physical_status.in_(statuses['physical']),)

#   the_query = ( session.query(*cars_cols).
#       join(Makes, Cars.c.car_manufacturer_id == Makes.c.car_manufacturer_id).
#       filter(inventory_conditions).
#       statement )

#   the_inventory = pd.read_sql(the_query, engine)
#   return the_inventory