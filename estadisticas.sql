SELECT count(*), avg(precio), min(precio), max(precio)5 FROM compras
--where precio is not null
order by id