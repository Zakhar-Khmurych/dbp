select * from customers;
select * from orders;
select * from order_items;


CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);




create index customer_id on orders (customer_id);
create index order_id on order_items (order_id);


select cust.name, cust.email, ordr.order_date, ordr.amount, ordr_itm.product_name, ordr_itm.quantity, ordr_itm.price
from customers cust
join orders ordr on cust.id = ordr.customer_id
join order_items ordr_itm on ordr.id = ordr_itm.order_id
where ordr.amount between 1000 and 2000

with customerOrders as (
    select ordr.id, ordr.customer_id, ordr.order_date
    from orders ordr
    where ordr.amount between 1000 and 2000
)
select cust.name, cust.email, cst_ordr.order_date, ordr_itm.product_name, ordr_itm.quantity, ordr_itm.price
from customers cust
join customerOrders cst_ordr on cust.id = cst_ordr.customer_id
join order_items ordr_itm on cst_ordr.id = ordr_itm.order_id;





