
import time
from threading import Thread


class Consumer(Thread):

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):

        self.kwargs = kwargs
        self.retry_wait_time = retry_wait_time
        self.carts = carts
        self.marketplace = marketplace
        Thread.__init__(self, **kwargs)

    def run(self):
        new_id_cart = self.marketplace.new_cart()
        for list_cart in self.carts:
            for dictionary in list_cart:
                action_type = dictionary.get("type")
                id_product = dictionary.get("product")
                quantity = dictionary.get("quantity")

                match action_type:
                    case "add":
                        iterator = 0
                        wait_time = True
                        while iterator < quantity:
                            wait_time = self.marketplace.add_to_cart(new_id_cart, id_product)
                            if wait_time:
                                iterator += 1
                            else:
                                time.sleep(self.retry_wait_time)
                    case _:
                        iterator = 0
                        while iterator < quantity:
                            self.marketplace.remove_from_cart(new_id_cart, id_product)
                            iterator += 1

        print_list = self.marketplace.place_order(new_id_cart)
        print_list.reverse()
        for product in print_list:
            print(self.name, "bought", product)
