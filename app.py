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
ACCEPT_HEADER_VALUE = (
    f"Accept: application/json; version={TIDIO_ACCEPT_API_VERSION}"
)

# Magento
MAGENTO_API_DOMAIN = os.getenv("WEB_API_DOMAIN")
MAGENTO_DOMAIN = os.getenv("WEB_DOMAIN")
TIMEZONE = pendulum.timezone("Europe/London")
UPDATE_AGE_MINS = os.getenv("UPDATE_AGE_MINS")


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
            "searchCriteria[filter_groups][1][filters][0][field]": "status",
            "searchCriteria[filter_groups][1][filters][0][value]": 1,
            "searchCriteria[filter_groups][1][filters][0][condition_type]": "eq",
            "fields": self.mag_product_fields,
        }
        self.mag_products_updated_criteria = {
            "searchCriteria[filter_groups][0][filters][0][field]": "updated_at",
            "searchCriteria[filter_groups][0][filters][0][value]": None,
            "searchCriteria[filter_groups][0][filters][0][condition_type]": "gteq",
        }
        self.mag_products_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_PRODUCTS_API_ENDPOINT"
        )
        self.mag_categories_ep = MAGENTO_API_DOMAIN + os.getenv(
            "MAG_CATEGORIES_API_ENDPOINT"
        )
        self.category_id_name_map = {}

    def fetch_web_products(self, full: bool = False) -> list:
        product_criteria = self.mag_product_criteria
        if not full:
            product_criteria |= self.mag_products_updated_criteria
            time_now = pendulum.now(tz=TIMEZONE)
            updated_after = time_now.subtract(minutes=int(UPDATE_AGE_MINS))
            updated_after_str = updated_after.to_datetime_string()
            product_criteria[
                "searchCriteria[filter_groups][0][filters][0][value]"
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
        self, product_media: dict
    ) -> str | None:
        if not isinstance(product_media, dict) or not product_media:
            raise ValueError("Provide the product's 'media_gallery_entries'.")
        if len(product_media) == 0:
            return None
        for media in product_media:
            if "image" in media["types"]:
                return (
                    f"{MAGENTO_DOMAIN}/media/catalog/products/{media['file']}"
                )
        return None

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
            "Product custom attributes do not include a {attribute}."
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

    def determine_web_product_price(self, product: dict) -> float:
        # How?
        pass


if __name__ == "__main__":
    magento = MagentoCatalog()
    updates = magento.fetch_web_products()
    print(updates[0])
