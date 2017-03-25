import urlparse


def parse_url(url):
    """parses a url into multiple components in the url
    Args:
        url: the url

    Returns:
        (tuple): the components of the url

    """
    return urlparse.urlparse(url)
