import novaposhta as np
import ukrposhta as up
import threading
import time


def start_update_catalog():

    threads = []

    key = "f711f674afa103c78970c025d46ef8ed"
    url = "https://api.novaposhta.ua/v2.0/json/"
    np_threads = threading.Thread(target=np.update_catalog, args=(url, key, None))
    threads.append(np_threads)
    np_threads.start()

    key = "38dfd023-2571-339b-81da-ef16b13ec736"
    url = "https://www.ukrposhta.ua/address-classifier-ws/"
    up_threads = threading.Thread(target=up.update_catalog, args=(url, key, None))
    threads.append(up_threads)
    up_threads.start()

    for t in threads:
        t.join()


if __name__ == '__main__':

    start = time.time()
    start_update_catalog()
    print("Общее время работы: ", int(int(time.time() - start)/60))
