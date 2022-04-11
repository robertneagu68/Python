import logging
import unittest
from logging.handlers import RotatingFileHandler
from threading import Lock


class Marketplace:

    def __init__(self, queue_size_per_producer):

        self.queue_size_per_producer = queue_size_per_producer
        self.id_producer = 0
        self.producer_dictionary = {}
        self.publish_lock = Lock()
        self.id_consumer = 0
        self.cart_dictionary = {}
        self.add_cart_lock = Lock()
        # Initializarea logging-ului
        logging.basicConfig(handlers=[RotatingFileHandler("marketplace.log", maxBytes=100000, backupCount=3)],
                            level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt="%d-%b-%y %H:%M:%S")

    def register_producer(self):
        logging.info("Entered register_producer()")
        # In cazul in care id-ul a fost doar initializat, i se atribuie valoarea 1
        # In orice alt caz, se incrementeaza cu 1 peste lungimea dictionarului de producatori
        match self.id_producer:
            case 0:
                self.id_producer = 1
            case _:
                self.id_producer = len(self.producer_dictionary) + 1

        # Initializam dictionarul de producatori ca o lista goala
        self.producer_dictionary[self.id_producer] = []

        logging.info("Function register_producer() returned: %d", self.id_producer)

        return self.id_producer

    def publish(self, producer_id, product):
        logging.info("Entering publish() function. Parameters: producer_id = %d, product = %s", producer_id, product)
        self.publish_lock.acquire()

        # Folosim variabila booleana available_queue pentru a tine cont daca un producator
        # poate publica pe market produsul, tinand cont de de queue_size_per_producer
        available_queue = False

        producer_list = self.producer_dictionary.get(producer_id)

        if len(producer_list) < self.queue_size_per_producer:
            self.producer_dictionary.get(producer_id).append(product)
            available_queue = True

        # In caz afirmativ eliberam lock-ul, altfel asteptam pentru a putea republica un produs
        match available_queue:
            case True:
                self.publish_lock.release()
                logging.info("Functia publish() returneaza: %s", available_queue)
                return True
            case False:
                logging.info("Functia publish() returneaza: %s", available_queue)
                return False

    def new_cart(self):

        # Incrementam id-ul consumatorului pentru a initializa un nou cart
        self.id_consumer += 1
        self.cart_dictionary[self.id_consumer] = []

        return self.id_consumer

    def add_to_cart(self, cart_id, product):
        logging.info("Entered add_to_cart(). Parameters: cart_id = %d, product = %s", cart_id, product)
        search_product_id = None
        is_product_found = False

        self.add_cart_lock.acquire()

        producer_key_list = list(self.producer_dictionary.keys())

        for key in producer_key_list:
            for prod in self.producer_dictionary.get(key):
                if prod == product:
                    search_product_id = key
                    is_product_found = True
                    break

        match is_product_found:
            case True:
                self.producer_dictionary.get(search_product_id).remove(product)
                self.cart_dictionary.get(cart_id).append([product, search_product_id])
                self.add_cart_lock.release()
                logging.info("Function add_to_cart() returned: %s", is_product_found)
                return is_product_found
            case False:
                self.add_cart_lock.release()
                logging.info("Function add_to_cart() returned: %s", is_product_found)
                return is_product_found

    def remove_from_cart(self, cart_id, product):
        logging.info("Entering remove_from_cart(). Parameters: card_id = %d, product = %s", cart_id, product)
        cart_items = self.cart_dictionary.get(cart_id)
        id_product_found = None
        is_product_found = False

        for prod, id_prod in cart_items:
            if prod == product:
                id_product_found = id_prod
                is_product_found = True
                break

        match is_product_found:
            case True:
                cart_items.remove([product, id_product_found])
                self.producer_dictionary.get(id_product_found).append(product)
                self.cart_dictionary[cart_id] = cart_items
                logging.info("Function add_to_cart() returned: %s", is_product_found)
            case False:
                logging.info("Function add_to_cart() returned: %s", is_product_found)
                pass

    def place_order(self, cart_id):
        logging.info("Entering place_order(). Parameters: card_id = %d", cart_id)
        order = []
        products = self.cart_dictionary.get(cart_id)

        for prod, id_prod in products:
            order.append(prod)

        logging.info("Function place_order() returned: %s", order)
        return order


class TestMarketplace(unittest.TestCase):

    def setUp(self):
        self.marketplace = Marketplace(50)

    def test_register_producer(self):
        id_producer1 = self.marketplace.register_producer()
        self.assertEqual(id_producer1, 1)

        id_producer2 = self.marketplace.register_producer()
        self.assertEqual(id_producer2, 2)

        id_producer3 = self.marketplace.register_producer()
        self.assertEqual(id_producer3, 3)

    def test_new_cart(self):
        id_consumer1 = self.marketplace.new_cart()
        self.assertEqual(id_consumer1, 1)

        id_consumer2 = self.marketplace.new_cart()
        self.assertEqual(id_consumer2, 2)

        id_consumer3 = self.marketplace.new_cart()
        self.assertEqual(id_consumer3, 3)

    def test_publish(self):
        # Instantiem un nou obiect de tip Marketplace cu queue_size_per_producer 1 pentru a testa
        # cazul in care depasim marimea queue-ului
        from product import Tea, Coffee
        self.marketplace_publish = Marketplace(1)

        id_producer1 = self.marketplace_publish.register_producer()
        product1 = Tea("Tea", 50, "Black")
        test_publish1 = self.marketplace_publish.publish(id_producer1, product1)
        self.assertTrue(test_publish1)

        product2 = Coffee("Coffee", 30, "Acidity", "Medium")
        test_publish2 = self.marketplace_publish.publish(id_producer1, product2)
        self.assertFalse(test_publish2)

    def test_add_to_cart(self):
        from product import Tea, Coffee
        id_cart = self.marketplace.new_cart()
        id_producer = self.marketplace.register_producer()

        product = Tea("Tea", 50, "Black")
        product2 = Coffee("Coffee", 30, "Acidity", "Medium")
        product3 = Tea("Alternative Tea", 70, "Green")

        test_cart = self.marketplace.add_to_cart(id_cart, product)
        self.assertFalse(test_cart)

        self.marketplace.producer_dictionary[id_producer] = [product, product2]

        test_cart2 = self.marketplace.add_to_cart(id_cart, product)
        self.assertTrue(test_cart2)

        test_cart4 = self.marketplace.add_to_cart(id_cart, product3)
        self.assertFalse(test_cart4)

    def test_remove_from_cart(self):
        from product import Tea, Coffee
        id_cart = self.marketplace.new_cart()
        id_producer = self.marketplace.register_producer()

        product = Tea("Tea", 50, "Black")
        product2 = Coffee("Coffee", 30, "Acidity", "Medium")

        self.marketplace.cart_dictionary[id_cart] = [{product, id_producer}]

        self.marketplace.remove_from_cart(id_cart, product)
        self.assertEqual(self.marketplace.producer_dictionary.get(1), [])

        self.marketplace.cart_dictionary[id_cart] = [{id_producer, product}]
        self.marketplace.remove_from_cart(id_cart, product2)
        self.assertIsNotNone(self.marketplace.cart_dictionary[id_cart])
