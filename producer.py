
import time
from threading import Thread


class Producer(Thread):

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):

        self.kwargs = kwargs
        self.republish_wait_time = republish_wait_time
        self.products = products
        self.marketplace = marketplace
        Thread.__init__(self, **kwargs)

    def run(self):
        while True:
            new_id_producer = self.marketplace.register_producer()
            for prod in self.products:
                iterator = 0
                wait_publish = True
                while iterator < prod[1]:
                    wait_publish = self.marketplace.publish(new_id_producer, prod[0])
                    match wait_publish:
                        case True:
                            iterator += 1
                            time.sleep(prod[2])
                        case False:
                            time.sleep(self.republish_wait_time)
                        case _:
                            pass
