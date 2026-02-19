import argparse
import io
import itertools
import math
import mimetypes
from pathlib import Path
import time
from typing import List
import zipfile
from dotenv import load_dotenv
import logging
import json
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import os
import pendulum
import requests
import sys

import urllib

load_dotenv()

LOG_FORMAT = (
    "ts=%(asctime)s level=%(levelname)s logger=%(name)s "
    'pid=%(process)d thread=%(threadName)s msg="%(message)s"'
)

formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z")

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

file_handler = logging.FileHandler("tidio_products.log")
file_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

OUTPUT_FILE = os.getenv("OUTPUT_FILE")
MANIFEST_FOLDER_ID = os.getenv("Z_WD_MANIFEST_FOLDER_ID")
CHECKPOINT_EVERY_N_BATCHES = 5

# Tidio
TIDIO_CLIENT_KEY = "X-Tidio-Openapi-Client-Id"
TIDIO_CLIENT_SECRET_KEY = "X-Tidio-Openapi-Client-Secret"
TIDIO_CLIENT_ID = os.getenv("TIDIO_CLIENT_ID")
TIDIO_CLIENT_SECRET = os.getenv("TIDIO_CLIENT_SECRET")
TIDIO_MAX_REQ_PER_MIN = os.getenv("TIDIO_MAX_REQ_PER_MIN")
TIDIO_MAX_PRODUCTS_PER_REQ = 100
TIDIO_RATELIMIT_LIMIT_KEY = "x-ratelimit-limit"
TIDIO_RATELIMIT_REMAINING_KEY = "x-ratelimit-remaining"
TIDIO_ACCEPT_API_VERSION = os.getenv("TIDIO_ACCEPT_API_VERSION")
TIDIO_API_UPSERT_PRODUCT_ENDPOINT = "https://api.tidio.com/products/batch"
TIDIO_API_DELETE_PRODUCT_ENDPOINT = "https://api.tidio.com/products/"
ACCEPT_HEADER_VALUE = f"application/json; version={TIDIO_ACCEPT_API_VERSION}"

# Magento
MAGENTO_API_DOMAIN = os.getenv("WEB_API_DOMAIN")
MAGENTO_DOMAIN = os.getenv("WEB_DOMAIN")
TIMEZONE = pendulum.timezone("Europe/London")
UPDATE_AGE_MINS = os.getenv("UPDATE_AGE_MINS")
EXCLUDED_FEATURES = json.loads(os.getenv("EXCLUDED_FEATURES"))
MAG_BRAND_ATTRIBUTE_CODE = os.getenv("MAG_BRAND_ATTRIBUTE_CODE")
MAGENTO_WEBSITE_ID = os.getenv("MAG_WEBSITE_ID")
BATCHES_FILE = "saved_batches.json"


class MagentoCatalog:

    def __init__(self):
        self.mag_domain = os.getenv("WEB_DOMAIN")
        self.mag_headers = {
            "Authorization": os.getenv("WEB_AUTH_HEADER_VALUE"),
            os.getenv("WEB_SECRET_NAME"): os.getenv("WEB_SECRET_PASS"),
        }
        self.mag_product_fields = (
            "items["
            + "id,sku,name,status,updated_at,media_gallery_entries,"
            + "extension_attributes[configurable_product_links,website_ids],custom_attributes"
            + "]"
            + ",errors,message,code,trace,parameters,total_count"
        )
        # Make sure not to use filter_group[2] here as that is reserved for
        #   the `updated_at` group that may be merged in.
        self.mag_product_criteria = {
            "searchCriteria[currentPage]": 1,
            "searchCriteria[pageSize]": 200,
            "searchCriteria[filter_groups][0][filters][0][field]": "status",
            "searchCriteria[filter_groups][0][filters][0][value]": 1,  # enabled
            "searchCriteria[filter_groups][0][filters][0][condition_type]": "eq",
            "searchCriteria[filter_groups][1][filters][0][field]": "visibility",
            "searchCriteria[filter_groups][1][filters][0][value]": 4,  # catalog, search
            "searchCriteria[filter_groups][1][filters][0][condition_type]": "eq",
            "fields": self.mag_product_fields,
        }
        self.mag_products_updated_criteria = {
            "searchCriteria[filter_groups][2][filters][0][field]": "updated_at",
            "searchCriteria[filter_groups][2][filters][0][value]": None,
            "searchCriteria[filter_groups][2][filters][0][condition_type]": "gteq",
        }
        self.mag_products_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_PRODUCTS_API_ENDPOINT"
        )
        self.mag_categories_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_CATEGORIES_API_ENDPOINT"
        )
        self.mag_prices_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_PRICES_API_ENDPOINT"
        )
        self.mag_attribute_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_ATTRIBUTE_API_ENDPOINT"
        )
        self.mag_store_id = os.getenv("MAG_STORE_ID")
        self.category_id_name_map = {}  # {category_id: category name}
        self.attribute_value_label_map = {}
        self.attribute_options_map = (
            {}
        )  # {attribute_code: {option_value: label}}
        self.session = requests.Session()
        self.session.headers.update(self.mag_headers)

    def fetch_web_products(self, full: bool = False) -> list:
        product_criteria = dict(self.mag_product_criteria)
        if not full:
            product_criteria |= self.mag_products_updated_criteria
            time_now = pendulum.now(tz=TIMEZONE)
            updated_after = time_now.subtract(minutes=int(UPDATE_AGE_MINS))
            updated_after_str = updated_after.to_datetime_string()
            product_criteria[
                "searchCriteria[filter_groups][2][filters][0][value]"
            ] = updated_after_str

        page_size = int(product_criteria.get("searchCriteria[pageSize]", 200))

        all_products = []

        def _fetch_web_products(current_page: int = 1) -> list:
            logger.info(f"Fetching page number {current_page}.")
            params = dict(product_criteria)
            params["searchCriteria[currentPage]"] = current_page

            raw_response = self.session.get(
                self.mag_products_ep,
                params=params,
            )
            json_response = raw_response.json()
            if "total_count" not in json_response:
                if "errors" in json_response and (
                    len(json_response["errors"]) > 0
                ):
                    logger.info("Errors" + json.dumps(json_response["errors"]))

                elif "message" in json_response:
                    logger.info(
                        "Message" + json.dumps(json_response["message"])
                    )
                elif "items" in json_response and json_response["items"]:
                    logger.info(
                        "No product updates found since " + updated_after_str
                    )
                else:
                    logger.info(
                        "Something happened where the response didn't contain 'total_count' but 'items' wasn't NULL."
                    )
                logger.info(
                    f"Response status: {raw_response.status_code}, content: {raw_response.content}"
                )
                raise Exception("Something went wrong with fetching products.")

            return json_response

        first_products_page = _fetch_web_products(1)
        total_count = int(first_products_page["total_count"])

        if total_count == 0:
            if not full:
                logger.info(
                    "No product updates found since" + updated_after_str
                )
            else:
                logger.info("No product updates found.")
            return []

        total_pages = math.ceil(total_count / page_size)

        if not full:
            logger.info(
                f"Found {total_count} product updates since {updated_after_str}."
            )
        else:
            logger.info(f"Found {total_count} products.")

        all_products.extend(first_products_page.get("items", []))

        for page in range(2, total_pages + 1):
            response = _fetch_web_products(page)
            all_products.extend(response.get("items", []))

        return all_products

    def determine_web_product_status(self, product: dict) -> str:
        if not isinstance(product, dict) or not product:
            raise ValueError(
                "Provide a product dictionary to determine status."
            )
        return "visible"

    def iso8601_format_updated_at(self, updated_at: str) -> str:
        if not updated_at:
            raise ValueError("Provide the 'updated_at' time str to format.")
        product_updated_at = pendulum.from_format(
            updated_at, "YYYY-MM-DD HH:mm:ss"
        )
        return product_updated_at.to_datetime_string()

    def determine_web_product_image_url(
        self, product_media: list
    ) -> str | None:
        if not isinstance(product_media, list) or not product_media:
            raise ValueError("Provide the product's 'media_gallery_entries'.")
        if len(product_media) == 0:
            return None
        logger.debug("Determining main image for product.")
        for media in product_media:
            if "image" in media["types"]:
                return f"{MAGENTO_DOMAIN}/media/catalog/product{media['file']}"
        return None

    def determine_web_product_url(self, product: dict) -> str:
        if not isinstance(product, dict) or not product:
            raise ValueError("Please provide a product dictionary.")
        return self.fetch_web_product_attribute_value("url_key", product)

    def fetch_web_product_attribute_value(
        self, attribute: str, product: dict
    ) -> str:
        if not attribute or not isinstance(attribute, str):
            raise ValueError(
                "Please provide the code of an attribute to fetch."
            )
        if not isinstance(product, dict) or not product:
            raise ValueError(
                f"Please provide a product dictionary to determine the {attribute}."
            )
        if "custom_attributes" not in product:
            raise ValueError(
                "Product dictionary does not include custom attributes."
            )
        logger.debug(f"Fetching value for {attribute}.")
        for product_attribute in product["custom_attributes"]:
            if product_attribute["attribute_code"] == attribute:
                return product_attribute["value"]
        raise ValueError(
            f"Product custom attributes do not include a '{attribute}'."
        )

    def fetch_web_category_name(self, category_id: int | str) -> str:
        if not category_id or (
            not isinstance(category_id, int)
            and not isinstance(category_id, str)
        ):
            raise ValueError(
                "Please provide a category ID (int or str) to get the name of."
            )
        logger.debug(f"Fetching name of category ID {category_id}.")
        # memoization
        if category_id in self.category_id_name_map:
            return self.category_id_name_map[category_id]

        # fetch category from Magento
        category_endpoint = f"{self.mag_categories_ep}/{category_id}"
        raw_order_response = self.session.get(
            category_endpoint,
        )
        json_response = raw_order_response.json()
        category_name = json_response["name"]

        # add to memoization map
        self.category_id_name_map[category_id] = category_name

        return category_name

    def fetch_web_atrribute_value_label(
        self, attribute_code: str, option_id: int | str
    ) -> str | None:
        # memoization
        str_option_id = str(option_id)
        if attribute_code in self.attribute_options_map:
            return self.attribute_options_map[attribute_code].get(str_option_id)
        if not isinstance(attribute_code, str) or not attribute_code:
            raise ValueError("Please provide an attribute code.")
        if not option_id or (
            not isinstance(option_id, int) and not isinstance(option_id, str)
        ):
            raise ValueError(
                "Please provide an option id (int or str) to return the label of."
            )
        logger.debug(f"Fetching attribute value labels of {attribute_code}.")
        attribute_endpoint = f"{self.mag_attribute_ep}{attribute_code}/options"
        raw_attribute_response = self.session.get(attribute_endpoint)
        options = raw_attribute_response.json()

        self.attribute_options_map[attribute_code] = {
            opt["value"]: opt["label"] for opt in options
        }

        return self.attribute_options_map[attribute_code].get(str_option_id)

    def extract_features(self, product: dict) -> dict:
        if not isinstance(product, dict) or not dict:
            raise ValueError(
                "Please provide a product dictionary to extract features."
            )
        custom_attributes = product["custom_attributes"]
        features = {}
        for attr in custom_attributes:
            if attr["attribute_code"] in EXCLUDED_FEATURES:
                continue
            if attr["value"]:
                label = self.fetch_web_atrribute_value_label(
                    attr["attribute_code"], attr["value"]
                )
                value = label if label else attr["value"]
                key = attr["attribute_code"].replace("filt_", "")
                logger.debug(f"Adding feature {key}: {label}.")
                if 250 > len(value):
                    value = value[:249]
                features[key] = value
        return features

    def build_attribute_index(self, product: dict) -> dict:
        return {
            attr["attribute_code"]: attr["value"]
            for attr in product.get("custom_attributes", [])
        }

    def prefetch_all_categories(self) -> None:
        """Loads the full category tree into the memoization map in one request."""
        response = self.session.get(self.mag_categories_ep)

        def walk(node):
            self.category_id_name_map[node["id"]] = node["name"]
            self.category_id_name_map[str(node["id"])] = node[
                "name"
            ]  # handle str keys too
            for child in node.get("children_data", []):
                walk(child)

        walk(response.json())

    def fetch_all_prices(
        self, skus: list[str], id_to_sku: dict[int, str]
    ) -> dict[str, float | str]:
        sku_prices = {}
        chunk_size = 50
        for i in range(0, len(skus), chunk_size):
            chunk = skus[i : i + chunk_size]
            criteria = {
                "searchCriteria[filter_groups][0][filters][0][field]": "sku",
                "searchCriteria[filter_groups][0][filters][0][value]": ",".join(
                    chunk
                ),
                "searchCriteria[filter_groups][0][filters][0][condition_type]": "in",
                "store_id": self.mag_store_id,
                "currencyCode": "GBP",
                "fields": "items[id,price_info]",
            }
            response = self.session.get(self.mag_prices_ep, params=criteria)
            response.raise_for_status()
            for item in response.json().get("items") or []:
                sku = id_to_sku.get(item["id"])
                if not sku:
                    logger.warning(
                        f"Price response contained unknown product id {item['id']}, skipping."
                    )
                    continue
                sku_prices[sku] = item["price_info"]["extension_attributes"][
                    "tax_adjustments"
                ]["final_price"]
        return sku_prices


class TidioAPI:

    def __init__(self):
        self.headers = {
            TIDIO_CLIENT_KEY: TIDIO_CLIENT_ID,
            TIDIO_CLIENT_SECRET_KEY: TIDIO_CLIENT_SECRET,
            "accept": ACCEPT_HEADER_VALUE,
            "content-type": f"application/json; version={TIDIO_ACCEPT_API_VERSION}",
        }
        self.last_request_time = None

    def upsert_product_batch(self, products: list):
        if not isinstance(products, list) or not products:
            raise ValueError("Please provide products to upsert.")
        number_of_products = len(products)
        if number_of_products > 100:
            raise ValueError(
                f"Too many products in upsert to Tidio API. Maximum 100 per request, found {number_of_products}."
            )
        if self.last_request_time:
            elapsed = (
                pendulum.now("Europe/London")
                .diff(self.last_request_time)
                .in_seconds()
            )
            if elapsed < 7:
                time.sleep(7 - elapsed)
        self.last_request_time = pendulum.now("Europe/London")
        payload = json.dumps({"products": products})
        raw_response = requests.put(
            TIDIO_API_UPSERT_PRODUCT_ENDPOINT,
            headers=self.headers,
            data=payload,
        )
        try:
            raw_response.raise_for_status()
        except requests.HTTPError:
            logger.error(
                "Upsert failed with HTTP %s. Response body: %s",
                raw_response.status_code,
                raw_response.text,
            )
            raise
        logger.info(
            f"Upserted batch of {number_of_products} products to Tidio API."
        )
        logger.debug(raw_response)
        logger.debug(raw_response.content)


def parse_and_write_magento_products(full: bool = False) -> None:
    output_json = []
    try:
        magento = MagentoCatalog()
        updates = magento.fetch_web_products(full)
        logger.info("Pre-fetching categories & prices...")
        magento.prefetch_all_categories()
        # Pre-fetch all prices in bulk before the loop
        attrs_by_sku = {
            p["sku"]: magento.build_attribute_index(p) for p in updates
        }
        id_to_sku = {p["id"]: p["sku"] for p in updates}
        skus = [
            p["sku"]
            for p in updates
            if int(attrs_by_sku[p["sku"]].get("priceonapplication", 0)) != 1
        ]
        price_map = magento.fetch_all_prices(skus, id_to_sku)
        for product in updates:
            if (
                int(MAGENTO_WEBSITE_ID)
                not in product["extension_attributes"]["website_ids"]
            ):
                logger.info(f"Skipping non-website product: {product['sku']}.")
                continue
            logger.info(f"Processing {product['sku']}")
            attrs = magento.build_attribute_index(product)
            category_ids = attrs.get("category_ids", [])
            url_key = attrs.get("url_key", "")
            description = attrs.get("description", "")
            product_categories = []
            for id in category_ids:
                category_name = magento.fetch_web_category_name(id)
                product_categories.append(category_name)
            lowest_category_name = (
                product_categories[-1] if len(product_categories) > 0 else None
            )
            tidio_product = {
                "id": product["id"],
                "url": f"{MAGENTO_DOMAIN}/{url_key}",
                "sku": product["sku"],
                "title": product["name"],
                "status": magento.determine_web_product_status(product),
                "updated_at": magento.iso8601_format_updated_at(
                    product["updated_at"]
                ),
                "image_url": magento.determine_web_product_image_url(
                    product["media_gallery_entries"]
                ),
                "features": magento.extract_features(product),
                "description": description,
                "default_currency": "GBP",
                "price": price_map.get(product["sku"], "null"),
            }
            product_vendor = None
            try:
                product_vendor = magento.fetch_web_atrribute_value_label(
                    MAG_BRAND_ATTRIBUTE_CODE,
                    magento.fetch_web_product_attribute_value(
                        MAG_BRAND_ATTRIBUTE_CODE, product
                    ),
                )
            except Exception as e:
                logger.info("No brand value found for product.")
            if product_vendor:
                tidio_product["vendor"] = product_vendor
            if lowest_category_name:
                tidio_product["product_type"] = lowest_category_name
            output_json.append(tidio_product)
    except Exception as e:
        logger.error(f"Something went wrong getting the products. {e}")
    finally:
        logger.info("Writing output so far.")
        with open(OUTPUT_FILE, "w") as output_file:
            output_file.write(json.dumps(output_json))


class WorkDrive:
    """WorkDrive class utilises the Zoho WorkDrive REST API."""

    oauthlib_conn: OAuth2Session
    last_file_meta: dict

    def __init__(self) -> None:
        """Initialise the WorkDrive instance, primarily to setup the
        OAuth2.0 credentials and make sure they are authorised."""

        load_dotenv()
        client_id = os.getenv("Z_CLIENT_ID")
        client_secret = os.getenv("Z_CLIENT_SECRET")
        scope = os.getenv("Z_SCOPE")
        refresh_token = os.getenv("Z_REFRESH_TOKEN")

        client = BackendApplicationClient(
            client_id=client_id, refresh_token=refresh_token
        )
        self.oauthlib_conn = OAuth2Session(client=client)
        if not self.oauthlib_conn.authorized:
            self.oauthlib_conn.fetch_token(
                token_url=f"https://accounts.zoho.{os.getenv('Z_REGION')}/oauth/v2/token",
                client_id=client_id,
                client_secret=client_secret,
                scope=scope,
            )

    def find_folder(self, parent_folder_id: str, folder_name: str) -> str:
        """Find a WorkDrive folder ID in the given parent folder with
        the folder name as given. Return the folder ID."""
        parameters = {"filter[type]": "folder"}
        list_folder_contents_endpoint = f"https://www.zohoapis.{os.getenv('Z_REGION')}/workdrive/api/v1/files/{parent_folder_id}/files?"
        first = True
        for key in parameters.keys():
            if not first:
                list_folder_contents_endpoint = (
                    list_folder_contents_endpoint + "&"
                )
            first = False
            list_folder_contents_endpoint = (
                list_folder_contents_endpoint
                + f"{urllib.parse.quote_plus(key)}={parameters[key]}"
            )
        list_folder_resp = self.oauthlib_conn.get(list_folder_contents_endpoint)
        list_folder_resp.raise_for_status()
        for folder in list_folder_resp.json()["data"]:
            if folder["attributes"]["name"] == folder_name:
                return folder["id"]
        return None

    def find_or_create_folder(self, parent_id: str, folder_name: str) -> str:
        found_folder_id = self.find_folder(parent_id, folder_name)
        if found_folder_id:
            return found_folder_id
        return self.create_folder(parent_id, folder_name)

    def create_folder(self, parent_id: str, folder_name: str) -> str:
        """Create a WorkDrive folder in the given parent folder with
        the folder name as given. Return the folder ID."""
        post_data = {
            "data": {
                "attributes": {"name": folder_name, "parent_id": parent_id},
                "type": "files",
            }
        }
        create_folder_endpoint = f"https://www.zohoapis.{os.getenv('Z_REGION')}/workdrive/api/v1/files"
        response = self.oauthlib_conn.post(
            create_folder_endpoint, json=post_data
        )
        response.raise_for_status()
        return response.json()["data"]["id"]

    def get_last_file_id(self) -> str:
        return self.last_file_meta["attributes"]["resource_id"]

    def download_file(self, file_id: str) -> str:
        download_endpoint = f"https://download.zoho.{os.getenv('Z_REGION')}/v1/workdrive/download/{file_id}"
        file_resp = self.oauthlib_conn.get(download_endpoint)
        file_resp.raise_for_status()
        fileinfo_endpoint = f"https://www.zohoapis.{os.getenv('Z_REGION')}/workdrive/api/v1/files/{file_id}"
        fileinfo_resp = self.oauthlib_conn.get(fileinfo_endpoint)
        fileinfo_resp.raise_for_status()
        file_extn = fileinfo_resp.json()["data"]["attributes"]["extn"]
        temp_path = f"{file_id}"
        if file_extn == "zip":
            Path(temp_path).mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(
                io.BytesIO(file_resp.content)
            ) as zip_reference:
                zip_reference.extractall(temp_path)
            temp_path = temp_path + "/"
        else:
            with open(
                f"{temp_path}.{file_extn}", "w", encoding="utf-8"
            ) as temp_file:
                temp_file.write(file_resp.content.decode())
            temp_path = f"{temp_path}.{file_extn}"
        return temp_path

    def upload_file(
        self, location_id: str, file_path: str, delete_local: bool = True
    ) -> str:
        """Uploads the file at the given filepath to the folder location indicated
        by the location_id parameter. Uses mimetypes to auto-detect type. If a file
        already exists with the same name, WorkDrive automatically appends a
        timestamp to the end."""

        file_type = mimetypes.guess_type(file_path)
        file_name = Path(file_path).name
        url = f"https://www.zohoapis.{os.getenv('Z_REGION')}/workdrive/api/v1/upload"
        payload = {"parent_id": location_id, "override-name-exist": "false"}
        try:
            with open(file_path, "rb") as file:
                files = [("content", (file_name, file.read(), file_type))]
        except OSError as e:
            print(e)
        response = self.oauthlib_conn.post(url, data=payload, files=files)

        if response.status_code != 200:
            print(response.status_code, response.content)
            return

        if delete_local:
            os.remove(file_path)

        self.last_file_meta = response.json()["data"][0]

        return response.json()["data"][0]["attributes"]["Permalink"]

    def get_locations(self) -> List:
        """Fetches a list of tuples which represent subfolder names & IDs
        from the defined root folder and all subfolders one level deep
        from those."""

        locations = [
            (
                os.getenv("Z_WD_ROOT_FOLDER_NAME"),
                os.getenv("Z_WD_ROOT_FOLDER_ID"),
            )
        ]
        folders = self._list_folders(locations[0][1])
        for folder in folders:
            id = folder["id"]
            name = folder["attributes"]["name"]
            locations.append((name, id))
            subfolders = self._list_folders(id)
            for subfolder in subfolders:
                subf_name = f"{name} > {subfolder['attributes']['name']}"
                locations.append((subf_name, subfolder["id"]))
        return locations

    def _list_folders(self, id) -> List:
        """Fetches a list of folders within the folder with the given
        id parameter. Returns a list of tuples with the folder name &
        IDs."""

        fields = "id,type,name"
        r = self.oauthlib_conn.get(
            f"https://www.zohoapis.{os.getenv('Z_REGION')}/workdrive/api/v1/files/{id}/files?filter%5Btype%5D=folder&fields%5Bfiles%5D="
            + fields
        )
        if r.status_code != 200:
            return []
        return json.loads(r.content)["data"]


def create_batches(products):
    logger.info("Batching products in preparation for API...")
    # Batch products
    batched_products = [
        list(b) for b in itertools.batched(products, TIDIO_MAX_PRODUCTS_PER_REQ)
    ]

    logger.info("Saving batches to disk...")
    # Save batches to disk
    manifest = {
        "meta": {
            "total_products": len(products),
            "total_batches": len(batched_products),
            "created_at": pendulum.now("Europe/London").to_iso8601_string(),
        },
        "batches": [
            {
                "index": i,
                "size": len(b),
                "status": "pending",
                "sent_at": None,
                "products": b,
            }
            for i, b in enumerate(batched_products)
        ],
    }

    with open(BATCHES_FILE, "w") as f:
        json.dump(manifest, f)


def send_batches(manifest: dict, wd: WorkDrive) -> bool:
    """Send all pending batches. Returns True if all sent successfully."""
    tidio = TidioAPI()
    total = manifest["meta"]["total_batches"]
    all_ok = True

    for batch_entry in manifest["batches"]:
        if batch_entry["status"] == "sent":
            logger.info(
                f"Skipping batch {batch_entry['index']}/{total} (already sent)"
            )
            continue

        try:
            tidio.upsert_product_batch(batch_entry["products"])
            batch_entry["status"] = "sent"
            batch_entry["sent_at"] = pendulum.now(
                "Europe/London"
            ).to_iso8601_string()
            logger.info(
                f"Sent batch {batch_entry['index'] + 1}/{total} ({batch_entry['size']} products)"
            )
        except Exception as e:
            batch_entry["status"] = "failed"
            all_ok = False
            logger.error(f"Batch {batch_entry['index']} failed: {e}")
        finally:
            # Always flush to disk
            with open(BATCHES_FILE, "w") as f:
                json.dump(manifest, f)

        # Periodic WorkDrive checkpoint during long runs
        sent_count = sum(
            1 for b in manifest["batches"] if b["status"] == "sent"
        )
        if sent_count % CHECKPOINT_EVERY_N_BATCHES == 0:
            upload_manifest(wd, manifest)

    return all_ok


def upload_manifest(wd: WorkDrive, manifest: dict) -> str:
    """Write manifest to disk and upload to WorkDrive. Returns permalink."""
    with open(BATCHES_FILE, "w") as f:
        json.dump(manifest, f)
    permalink = wd.upload_file(
        MANIFEST_FOLDER_ID, BATCHES_FILE, delete_local=False
    )
    file_id = wd.get_last_file_id()
    logger.info(
        f"Manifest uploaded to WorkDrive. File ID: {file_id} | URL: {permalink}"
    )
    return file_id


def download_manifest(wd: WorkDrive, file_id: str) -> dict:
    """Download a manifest from WorkDrive and return it as a dict."""
    local_path = wd.download_file(file_id)
    with open(local_path, "r") as f:
        return json.load(f)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", default=False)
    parser.add_argument(
        "--resume", type=str, default=None, metavar="WORKDRIVE_FILE_ID"
    )
    args = parser.parse_args()

    wd = WorkDrive()

    if args.resume:
        logger.info(f"Resuming from WorkDrive manifest: {args.resume}")
        manifest = download_manifest(wd, args.resume)
        pending = sum(1 for b in manifest["batches"] if b["status"] != "sent")
        logger.info(
            f"Resuming: {pending} of {manifest['meta']['total_batches']} batches remaining"
        )
    else:
        logger.info(
            f"Starting {'full' if args.full else 'incremental'} sync..."
        )
        parse_and_write_magento_products(full=args.full)

        with open(OUTPUT_FILE, "r") as f:
            products = json.load(f)

        batched = [
            list(b)
            for b in itertools.batched(products, TIDIO_MAX_PRODUCTS_PER_REQ)
        ]
        manifest = {
            "meta": {
                "total_products": len(products),
                "total_batches": len(batched),
                "created_at": pendulum.now("Europe/London").to_iso8601_string(),
                "sync_type": "full" if args.full else "incremental",
            },
            "batches": [
                {
                    "index": i,
                    "size": len(b),
                    "status": "pending",
                    "sent_at": None,
                    "products": b,
                }
                for i, b in enumerate(batched)
            ],
        }

    all_ok = send_batches(manifest, wd)

    if all_ok:
        logger.info("Sync completed successfully.")
        upload_manifest(wd, manifest)  # final record
    else:
        failed = [
            b["index"] for b in manifest["batches"] if b["status"] == "failed"
        ]
        file_id = upload_manifest(wd, manifest)
        logger.error(
            f"Sync completed with failures on batches {failed}. "
            f"To resume, run: python app.py --resume {file_id}"
        )
        sys.exit(1)  # non-zero so can detect failure
