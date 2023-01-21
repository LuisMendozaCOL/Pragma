from sqlalchemy import create_engine, Column, Integer, String, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base


# Creamos una clase llamada "MyTable" que hereda los atributos de la clase declarative_base.
Base = declarative_base()
# Esta clase tiene una tabla llamada "compras" con los atributos "id", "id_usuario", "fecha" y "precio".
class MyTable(Base):
    __tablename__ = "compras"
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_usuario = Column(Integer)
    fecha = Column(Date)
    precio = Column(Integer)
    UniqueConstraint(fecha, precio, id_usuario)


def crear_base_de_datos(engine):
    # Finalmente, utilizamos el método "create_all" para crear la tabla "compras" en la base de datos "Pragma.db".
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    engine = create_engine("sqlite:///Pragma.db")
    crear_base_de_datos(engine)
