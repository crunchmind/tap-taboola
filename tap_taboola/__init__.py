#!/usr/bin/env python3
import datetime
import os
import json
from singer import Transformer, utils, metadata, get_logger, write_schema, write_record
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
import backoff
import multiprocessing
import concurrent.futures
import random

REQUIRED_CONFIG_KEYS = ['start_date', 'account_id']
ACCESS_TOKEN_CONFIG_KEY = 'access_token'
GENERATE_TOKEN_CONFIG_KEYS = ['username', 'password', 'client_id', 'client_secret']
BASE_URL = 'https://backstage.taboola.com'
LOGGER = get_logger()


class TapTaboolaException(Exception):
    pass


def create_thread_pool_executor(max_workers=multiprocessing.cpu_count() * 5):
    return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)


def wait_all(futures):
    results = []
    for future in futures:
        try:
            results.append(future.result())
        except Exception as e:
            LOGGER.critical(e)

    return results


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, access_token, params={}):
    LOGGER.info("Making request: GET {} {}".format(url, params))

    try:
        response = requests.get(
            url,
            headers={'Authorization': 'Bearer {}'.format(access_token),
                     'Accept': 'application/json'},
            params=params)
    except Exception as exception:
        LOGGER.exception(exception)

    LOGGER.info("Got response code: {}".format(response.status_code))

    response.raise_for_status()
    return response


def get_token_password_auth(client_id, client_secret, username, password):
    url = '{}/backstage/oauth/token'.format(BASE_URL)
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password,
        'grant_type': 'password',
    }

    response = requests.post(
        url,
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'},
        params=params)

    LOGGER.info("Got response code: {}".format(response.status_code))

    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get('access_token', None)}
    elif response.status_code >= 400 and response.status_code < 500:
        result = {k: response.json().get(k) for k in ('error','error_description')}
    else:
        result = {}

    return result


def get_token_client_credentials_auth(client_id, client_secret):
    url = '{}/backstage/oauth/token'.format(BASE_URL)
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }

    response = requests.post(
        url,
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'},
        params=params)

    LOGGER.info("Got response code: {}".format(response.status_code))

    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get('access_token', None)}
    elif 400 <= response.status_code < 500:
        result = {k: response.json().get(k) for k in ('error', 'error_description')}
    else:
        result = {}

    return result


def generate_token(client_id, client_secret, username, password):
    LOGGER.info("Generating new token with password auth")
    token_result = get_token_password_auth(client_id, client_secret, username, password)
    if 'token' not in token_result:
        LOGGER.info("Retrying with client credentials authentication.")
        token_result = get_token_client_credentials_auth(client_id, client_secret)

    token = token_result.get('token')
    if token is None:
        raise Exception('Unable to authenticate, response from Taboola - {}: {}'
                        .format(token_result.get('error'),
                                token_result.get('error_description')))

    return token


def verify_account_access(config):
    account_id = config.get('account_id')
    access_token = config.get('access_token')
    """
    Fetch a list of current user's permitted accounts
    See Backstage docs: Users Section 1.2.2
    """
    url = '{}/backstage/api/1.0/users/current/allowed-accounts/'.format(BASE_URL)

    response = request(url, access_token)
    results = response.json().get('results')
    for account in results:
        if account['account_id'] == account_id and account.get('is_active') is True:
            LOGGER.info("Verified account access via token details endpoint.")
            return True

    return False


def validate_config(config):
    missing_keys = []
    null_keys = []
    has_errors = False

    if not config.get(ACCESS_TOKEN_CONFIG_KEY):
        LOGGER.info('Config is missing access token, checking for generate token keys')
        for required_key in GENERATE_TOKEN_CONFIG_KEYS:
            if required_key not in config:
                missing_keys.append(required_key)

            elif config.get(required_key) is None:
                null_keys.append(required_key)

    if missing_keys:
        LOGGER.fatal("Config is missing keys: {}"
                     .format(", ".join(missing_keys)))
        has_errors = True

    if null_keys:
        LOGGER.fatal("Config has null keys: {}"
                     .format(", ".join(null_keys)))
        has_errors = True

    if has_errors:
        raise RuntimeError


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.tap_stream_id))
    schema = utils.load_json(path)
    return schema


def initialize_stream(stream_id, schema):
    """
    :param stream_id:
    :param schema:
    :return: key_properties, stream_metadata
    """
    key_properties = []
    stream_metadata = []

    if stream_id in ['campaigns', 'items']:
        key_properties = ['id']
        stream_metadata = metadata.get_standard_metadata(schema, key_properties=key_properties)
    elif stream_id in ['campaign_day_performance', 'campaign_hourly_performance']:
        key_properties = ['campaign']
        stream_metadata = metadata.get_standard_metadata(schema, key_properties=key_properties)

    return key_properties, stream_metadata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        key_properties, stream_metadata = initialize_stream(stream_id, schema.to_dict())
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None
            )
        )
    return Catalog(streams)


def fetch_campaigns(config):
    account_id = config.get('account_id')
    access_token = config.get('access_token')

    url = '{}/backstage/api/1.0/{}/campaigns/'.format(BASE_URL, account_id)

    response = request(url, access_token)
    return response.json().get('results')


def fetch_campaign_day_performance(config, state):
    account_id = config.get('account_id')
    access_token = config.get('access_token')

    url = (
        '{}/backstage/api/1.0/{}/reports/campaign-summary/dimensions/campaign_day_breakdown'  # pylint: disable=line-too-long
        .format(BASE_URL, account_id))

    params = {
        'start_date': state.get('start_date', config.get('start_date')),
        'end_date': config.get('end_date', datetime.datetime.utcnow().date()),
    }

    campaign_performance = request(url, access_token, params)
    return campaign_performance.json().get('results')


def fetch_campaign_hourly_performance(config, state):
    account_id = config.get('account_id')
    access_token = config.get('access_token')

    url = (
        '{}/backstage/api/1.0/{}/reports/campaign-summary/dimensions/campaign_hour_breakdown'  # pylint: disable=line-too-long
        .format(BASE_URL, account_id))

    params = {
        'start_date': state.get('start_date', config.get('start_date')),
        'end_date': config.get('end_date', datetime.datetime.utcnow().date()),
    }

    campaign_performance = request(url, access_token, params)
    return campaign_performance.json().get('results')


def fetch_campaign_items(config, campaign_id):
    account_id = config.get('account_id')
    access_token = config.get('access_token')

    url = ('{}/backstage/api/1.0/{}/campaigns/{}/items/'.format(BASE_URL, account_id, campaign_id))

    response = request(url, access_token)
    return response.json().get('results')


def sync_campaign_items(config, state, stream, transformer, schema, map_metadata, campaign_id, time_extracted):
    campaign_items = fetch_campaign_items(config, campaign_id)
    for campaign_item in campaign_items:
        record = transformer.transform(campaign_item, schema, metadata=map_metadata)
        write_record(stream.tap_stream_id, record, stream.stream_alias, time_extracted)


def sample_percent_paused_campaigns(paused_campaigns_ids, sample_percent):
    random.shuffle(paused_campaigns_ids)
    amount_of_campaigns = round(len(paused_campaigns_ids) * (sample_percent / 100))
    LOGGER.info(f'sampling {amount_of_campaigns} paused campaigns')
    return paused_campaigns_ids[:amount_of_campaigns]


def sync_campaigns_items(config, state, stream, schema, map_metadata):
    campaigns = fetch_campaigns(config)
    campaign_ids = [c['id'] for c in campaigns if c['status'] not in ['PAUSED', 'TERMINATED']]
    paused_campaigns_ids = [c['id'] for c in campaigns if c['status'] == 'PAUSED']

    if config.get('sampling_percent_paused_campaigns_items') and \
            isinstance(config['sampling_percent_paused_campaigns_items'], int):
        paused_campaigns_ids = \
            sample_percent_paused_campaigns(paused_campaigns_ids, config['sampling_percent_paused_campaigns_items'])

    campaign_ids.extend(paused_campaigns_ids)
    time_extracted = utils.now()
    transformer = Transformer()
    executor = create_thread_pool_executor(max_workers=config.get('items_max_workers', 5))
    futures = []
    for campaign_id in campaign_ids:
        futures.append(executor.submit(sync_campaign_items, config, state, stream, transformer, schema, map_metadata,
                                       campaign_id, time_extracted))

    wait_all(futures)


def sync(config, state, catalog):
    """ Sync data from tap source """
    if not verify_account_access(config):
        raise TapTaboolaException(f'Could not verify account id: {config["account_id"]}')
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        results = []
        schema = load_schema(stream)
        map_metadata = metadata.to_map(stream.metadata)
        write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )

        if stream.tap_stream_id == 'campaigns':
            results = fetch_campaigns(config)
        elif stream.tap_stream_id == 'campaign_day_performance':
            results = fetch_campaign_day_performance(config, state)
        elif stream.tap_stream_id == 'campaign_hourly_performance':
            results = fetch_campaign_hourly_performance(config, state)
        elif stream.tap_stream_id == 'items':
            sync_campaigns_items(config, state, stream, schema, map_metadata)
        else:
            raise TapTaboolaException('Unknown stream {}'.format(stream.tap_stream_id))

        time_extracted = utils.now()
        with Transformer() as transformer:
            for result in results:
                record = transformer.transform(result, schema, metadata=map_metadata)
                write_record(stream.tap_stream_id, record, stream.stream_alias, time_extracted)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    validate_config(config)
    if not config.get('access_token'):
        config['access_token'] = generate_token(config['client_id'], config['client_secret'], config['username'],
                                                config['password'])

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    elif args.catalog:
        catalog = args.catalog
        sync(args.config, args.state, catalog)
    else:
        LOGGER.critical("No catalog were selected")


if __name__ == "__main__":
    main()
