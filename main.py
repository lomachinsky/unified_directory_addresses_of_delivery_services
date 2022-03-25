import novaposhta as np
import ukrposhta as up
import threading
import time
from config import settings_novaposhta as set_np
from config import settings_ukrposhta as set_up


def start_update_catalog():

    threads = []

    np_threads = threading.Thread(target=np.update_catalog, args=(set_np.url, set_np.key, None))
    threads.append(np_threads)
    np_threads.start()

    up_threads = threading.Thread(target=up.update_catalog, args=(set_up.url, set_up.key, None))
    threads.append(up_threads)
    up_threads.start()

    for t in threads:
        t.join()


if __name__ == '__main__':

    start = time.time()
    start_update_catalog()
    print("Общее время: ", int(int(time.time() - start)/60))
