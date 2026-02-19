import itertools
import math
import time
from dotenv import load_dotenv
import logging
import json
import os
import pendulum
import requests
import sys

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
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

OUTPUT_FILE = os.getenv("OUTPUT_FILE")

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

    def fetch_all_prices(self, skus: list[str]) -> dict[str, float | str]:
        """Returns {sku: price} for all SKUs in as few requests as possible."""
        sku_prices = {}
        # Magento URLs can get long; chunk SKUs to be safe
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
                "fields": "items[sku,price_info]",
            }
            response = self.session.get(self.mag_prices_ep, params=criteria)
            for item in response.json().get("items", []):
                sku = item["sku"]
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
        if raw_response.status_code == 400:
            logger.error("Upsert failed with HTTP Error 400.")
            raise requests.HTTPError("Bad request, check the payload.")
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
        magento.prefetch_all_categories()
        # Pre-fetch all prices in bulk before the loop
        attrs_by_sku = {
            p["sku"]: magento.build_attribute_index(p) for p in updates
        }
        skus = [
            p["sku"]
            for p in updates
            if int(attrs_by_sku[p["sku"]].get("priceonapplication", 0)) != 1
        ]
        price_map = magento.fetch_all_prices(skus)
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


if __name__ == "__main__":

    logger.info("Starting...")
    # Get and prepare products
    parse_and_write_magento_products(full=True)

    logger.info("Reading products from file...")
    # Read products
    with open(OUTPUT_FILE, "r") as input_file:
        products = json.loads(input_file.read())

    logger.info("Batching products in preparation for API...")
    # Batch products
    batched_products = [
        list(batch)
        for batch in itertools.batched(products, TIDIO_MAX_PRODUCTS_PER_REQ)
    ]

    logger.info("Saving batches to disk...")
    # Save batches to disk
    with open("saved_batches.json", "w") as f:
        json.dump(batched_products, f)

    # Send batches to Tidio
    tidio = TidioAPI()
    for i, batch in enumerate(batched_products, 1):
        logger.info(f"Sending batch {i}: {len(batch)} products")
        # tidio.upsert_product_batch(batch)
