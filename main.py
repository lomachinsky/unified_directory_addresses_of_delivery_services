import novaposhta as np
import ukrposhta as up
import threading
import time
import additional as adt
import config


def start_update_catalog():

    threads = []

    set_np = config.settings_novaposhta
    np_threads = threading.Thread(target=np.update_catalog, args=(set_np['url'], set_np['key'], None))
    threads.append(np_threads)
    np_threads.start()

    set_up = config.settings_ukrposhta
    up_threads = threading.Thread(target=up.update_catalog, args=(set_up['url'], set_up['key'], None))
    threads.append(up_threads)
    up_threads.start()

    for t in threads:
        t.join()


if __name__ == '__main__':

    start = time.time()
    start_update_catalog()
    print("Total time: ", adt.calculate_time(start))
