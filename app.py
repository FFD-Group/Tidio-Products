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

# Tidio
TIDIO_CLIENT_KEY = "X-Tidio-Openapi-Client-Id"
TIDIO_CLIENT_SECRET_KEY = "X-Tidio-Openapi-Client-Secret"
TIDIO_CLIENT_ID = os.getenv("TIDIO_CLIENT_ID")
TIDIO_CLIENT_SECRET = os.getenv("TIDIO_CLIENT_SECRET")
TIDIO_MAX_REQ_PER_MIN = os.getenv("TIDIO_MAX_REQ_PER_MIN")
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
            + "extension_attributes[configurable_product_links],custom_attributes"
            + "]"
            + ",errors,message,code,trace,parameters,total_count"
        )
        self.mag_product_criteria = {
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
        self.category_id_name_map = {}
        self.attribute_value_label_map = {}

    def fetch_web_products(self, full: bool = False) -> list:
        product_criteria = self.mag_product_criteria
        if not full:
            product_criteria |= self.mag_products_updated_criteria
            time_now = pendulum.now(tz=TIMEZONE)
            updated_after = time_now.subtract(minutes=int(UPDATE_AGE_MINS))
            updated_after_str = updated_after.to_datetime_string()
            product_criteria[
                "searchCriteria[filter_groups][2][filters][0][value]"
            ] = updated_after_str

        raw_order_response = requests.get(
            self.mag_products_ep,
            headers=self.mag_headers,
            params=product_criteria,
        )
        json_response = raw_order_response.json()
        if "total_count" not in json_response:
            if "errors" in json_response and (len(json_response["errors"]) > 0):
                logger.info("Errors" + json.dumps(json_response["errors"]))

            elif "message" in json_response:
                logger.info("Message" + json.dumps(json_response["message"]))
            elif "items" in json_response and json_response["items"]:
                logger.info(
                    "No product updates found since " + updated_after_str
                )
            else:
                logger.info(
                    "Something happened where the response didn't contain 'total_count' but 'items' wasn't NULL."
                )
            logger.info("Exiting")
            sys.exit(0)
        elif json_response["total_count"] == 0:
            logger.info("No product updates found since " + updated_after_str)
            logger.info("Exiting")
            sys.exit(0)
        else:
            logger.info(
                "Found "
                + str(json_response["total_count"])
                + " product updates since "
                + updated_after_str
            )
        return list(json_response["items"])

    def determine_web_product_status(self, product: dict) -> str:
        if not isinstance(product, dict) or not product:
            raise ValueError(
                "Provide a product dictionary to determine status."
            )
        if "discontinued" in product["custom_attributes"]:
            if product["custom_attributes"]["discontinued"] != 0:
                return "hidden"
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
        for media in product_media:
            if "image" in media["types"]:
                return f"{MAGENTO_DOMAIN}/media/catalog/product/{media['file']}"
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
        for product_attribute in product["custom_attributes"]:
            if product_attribute["attribute_code"] == attribute:
                return product_attribute["value"]
        raise ValueError(
            "Product custom attributes do not include a '{attribute}'."
        )

    def fetch_web_category_name(self, category_id: int | str) -> str:
        if not category_id or (
            not isinstance(category_id, int)
            and not isinstance(category_id, str)
        ):
            raise ValueError(
                "Please provide a category ID (int or str) to get the name of."
            )

        # memoization
        if category_id in self.category_id_name_map:
            return self.category_id_name_map[category_id]

        # fetch category from Magento
        category_endpoint = f"{self.mag_categories_ep}/{category_id}"
        raw_order_response = requests.get(
            category_endpoint,
            headers=self.mag_headers,
        )
        json_response = raw_order_response.json()
        category_name = json_response["name"]

        # add to memoization map
        self.category_id_name_map[category_id] = category_name

        return category_name

    def determine_web_product_price(self, product: dict) -> float | str:
        if not isinstance(product, dict) or not product:
            raise ValueError(
                "Please provide a product dictionary to determine price."
            )
        if "custom_attributes" in product:
            poa = [
                attr["value"]
                for attr in product["custom_attributes"]
                if attr["attribute_code"] == "priceonapplication"
            ][0]
            if int(poa) == 1:
                return "null"
        criteria = {
            "searchCriteria[filter_groups][0][filters][0][field]": "sku",
            "searchCriteria[filter_groups][0][filters][0][value]": product[
                "sku"
            ],
            "store_id": self.mag_store_id,
            "currencyCode": "GBP",
            "fields": "items[price_info]",
        }
        prices_endpoint = f"{self.mag_prices_ep}"
        raw_prices_response = requests.get(
            prices_endpoint, headers=self.mag_headers, params=criteria
        )
        json_response = raw_prices_response.json()
        if not json_response["items"]:
            return "null"
        return json_response["items"][0]["price_info"]["extension_attributes"][
            "tax_adjustments"
        ]["final_price"]

    def fetch_web_atrribute_value_label(
        self, attribute_code: str, option_id: int | str
    ) -> str | None:
        # memoization
        key = attribute_code + str(option_id)
        if key in self.attribute_value_label_map:
            return self.attribute_value_label_map[key]
        if not isinstance(attribute_code, str) or not attribute_code:
            raise ValueError("Please provide an attribute code.")
        if not option_id or (
            not isinstance(option_id, int) and not isinstance(option_id, str)
        ):
            raise ValueError(
                "Please provide an option id (int or str) to return the label of."
            )
        attribute_endpoint = f"{self.mag_attribute_ep}{attribute_code}/options"
        raw_attribute_response = requests.get(
            attribute_endpoint, headers=self.mag_headers
        )
        json_response = raw_attribute_response.json()
        label = [
            opt["label"]
            for opt in json_response
            if opt["value"] == str(option_id)
        ]
        if len(label) == 0:
            self.attribute_value_label_map[key] = None  # memoize
            return None
        self.attribute_value_label_map[key] = label[0]  # memoize
        return label[0]

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
                features[key] = value
        return features


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
            time_now = pendulum.now("Europe/London")
            if time_now <= self.last_request_time.subtract(seconds=7):
                time.sleep(7)
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


if __name__ == "__main__":
    magento = MagentoCatalog()
    updates = magento.fetch_web_products()
    output_json = []
    # for product in updates:
    #     product_categories = []
    #     for attribute in product["custom_attributes"]:
    #         if attribute["attribute_code"] == "category_ids":
    #             for id in attribute["value"]:
    #                 category_name = magento.fetch_web_category_name(id)
    #                 product_categories.append(category_name)
    #     tidio_product = {
    #         "id": product["id"],
    #         "url": f"{MAGENTO_DOMAIN}/{magento.determine_web_product_url(product)}",
    #         "sku": product["sku"],
    #         "title": product["name"],
    #         "status": magento.determine_web_product_status(product),
    #         "updated_at": magento.iso8601_format_updated_at(
    #             product["updated_at"]
    #         ),
    #         "image_url": magento.determine_web_product_image_url(
    #             product["media_gallery_entries"]
    #         ),
    #         "features": magento.extract_features(product),
    #         "description": magento.fetch_web_product_attribute_value(
    #             "description", product
    #         ),
    #         "default_currency": "GBP",
    #         "vendor": magento.fetch_web_atrribute_value_label(
    #             MAG_BRAND_ATTRIBUTE_CODE,
    #             magento.fetch_web_product_attribute_value(
    #                 MAG_BRAND_ATTRIBUTE_CODE, product
    #             ),
    #         ),
    #         "product_type": product_categories[-1],
    #         "price": magento.determine_web_product_price(product),
    #     }
    #     output_json.append(tidio_product)
    #     print(product["sku"])
    # with open("output.json", "w") as output_file:
    #     output_file.write(json.dumps(output_json))

    with open("output.json", "r") as input_file:
        products = json.loads(input_file.read())

    tidio = TidioAPI()
    tidio.upsert_product_batch(products)
