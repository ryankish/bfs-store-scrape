import os
import sys
import argparse
import requests
import pandas as pd
import time
import random
import logging
from scipy.spatial import ConvexHull
from collections import deque
from shapely.geometry import Point, Polygon
from datetime import datetime


URL = 

def create_convex_hull(points):
    if len(points) < 3:
        raise ValueError('must have >= 3 points')
    hull = ConvexHull(points)
    polygon_points = [points[vertex] for vertex in hull.vertices]
    return Polygon(polygon_points)


def get_stores(lat, lng):
    resp = []
    for i in range(10):
        resp = requests.get(URL.format(lat, lng))
        try:
            resp = resp.json()
            break
        except:
            time.sleep(random.uniform(1, 5))
            continue
    raw_data = resp["Data"]
    return raw_data


def searchState(state, state_zip_coords, output_dir):
    logging.info(f'Starting scrape for state: {state}')
    start_time = time.time()

    n_zip_codes = len(state_zip_coords)
    unqueried_zip_codes = set(state_zip_coords)
    queried_zip_codes = set()
  
    starting_coord = state_zip_coords.copy().pop()
    seen_stores_coords = set()
    seen_stores_tuples = set()
    queried_coords = set()

    graph = None

    queue = deque()
    queue.append(starting_coord)

    iterations = 0
    zip_codes_queried = 0

    while queue:
        
        current_coord = queue.popleft()
        queried_coords.add(current_coord)

        if not graph or not graph.contains(Point(current_coord[0], current_coord[1])):
            iterations += 1
            new_coords = []
            closest_store_coords = []

            closest_stores = get_stores(current_coord[0], current_coord[1])
            for store in closest_stores:
                store_tuple = tuple(store.items())
                store_coord = (store['Latitude'], store['Longitude'])
                closest_store_coords.append(store_coord)
                if store_coord not in seen_stores_coords and store['State'] == state:
                    seen_stores_tuples.add(store_tuple)
                    seen_stores_coords.add(store_coord)
                    new_coords.append(store_coord)
            
                sys.stdout.write(f"\rStores found: {len(seen_stores_coords)}, Iterations: {iterations}")
                sys.stdout.flush()

            queue.extend(reversed(new_coords))

            if graph and not graph.contains(Point(current_coord[0], current_coord[1])):
                closest_store_coords.append(current_coord)

            if closest_store_coords:
                if graph is None:
                    graph = create_convex_hull(closest_store_coords)
                else:
                    new_hull = create_convex_hull(closest_store_coords)
                    graph = graph.union(new_hull)
    
        # if no more stores, add zipcodes that are not within the bounds of seen stores
        while not queue and state_zip_coords:
            next_zip_coord = state_zip_coords.pop()
            lat, lng = next_zip_coord
            if graph is None or not graph.contains(Point(lat, lng)):
                zip_codes_queried += 1
                queried_zip_codes.add(next_zip_coord)
                unqueried_zip_codes.remove(next_zip_coord)
                queue.append(next_zip_coord)
                queried_coords.add(next_zip_coord)
                assert graph, 'the graph should not be empty'
    
    logging.info(f'Finished scrape for state: {state} in {time.time() - start_time:.2f} seconds')
    logging.info(f'Number of iterations: {iterations}')
    logging.info(f'# Zip codes: {n_zip_codes}, Zip codes queried: {zip_codes_queried}')
    logging.info(f'# Stores: {len(seen_stores_coords)}')
    assert len(seen_stores_coords) == len(seen_stores_tuples), 'store tuples and store coords should be the same length'
    
    # tuples to df
    stores_dict_list = []
    for store_tuple in seen_stores_tuples:
        store_dict = {k: v for k, v in store_tuple}
        stores_dict_list.append(store_dict)

    stores_found_df = pd.DataFrame(stores_dict_list)
    stores_found_df.to_csv(os.path.join(output_dir, f'{state}_stores.csv'), index=False)
    return seen_stores_tuples

def main():
    parser = argparse.ArgumentParser(description='Scrape stores by state.')
    parser.add_argument('scrape_id', type=str, help='The scrape identifier.')
    args = parser.parse_args()

    scrape_id = args.scrape_id
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = os.path.join('scrapes', str(scrape_id), timestamp)
    os.makedirs(output_dir, exist_ok=True)

    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    log_filename = os.path.join(output_dir, 'scrape.log')
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(file_handler)
    logging.info(f"Starting scrape {scrape_id} at {timestamp}")
    
    zip_codes = pd.read_csv('zip_codes.csv')
    states = zip_codes['state'].unique()

    for state in states:
        try:
            state_zip_codes = zip_codes[zip_codes['state']==state].reset_index(drop=True)
            state_zip_coords = set(zip(state_zip_codes['latitude'], state_zip_codes['longitude']))
            stores_found = searchState(state, state_zip_coords, output_dir)
        except Exception as e:
            logging.error(f'Error scraping state {state}: {e}')
            continue

if __name__ == "__main__":
    main()