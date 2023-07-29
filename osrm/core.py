# -*- coding: utf-8 -*-
import numpy as np
from pandas import DataFrame
from polyline import encode as polyline_encode

from . import RequestConfig

try:
    from urllib.parse import quote
    from urllib.request import Request, urlopen
except:
    from urllib2 import urlopen, Request
    from urllib2 import quote

import json


def _chain(*lists):
    for li in lists:
        for elem in li:
            yield elem


def check_host(host):
    """Helper function to get the hostname in desired format"""
    if not ("http" in host and "//" in host) and host[len(host) - 1] == "/":
        return "".join(["http://", host[: len(host) - 1]])
    elif not ("http" in host and "//" in host):
        return "".join(["http://", host])
    elif host[len(host) - 1] == "/":
        return host[: len(host) - 1]
    else:
        return host


def table(
    coords_src,
    coords_dest=None,
    ids_origin=None,
    ids_dest=None,
    output="np",
    minutes=False,
    annotations="duration",
    url_config=RequestConfig,
    send_as_polyline=True,
):
    """
    Function wrapping OSRM 'table' function in order to get a matrix of
    time distance as a numpy array or as a DataFrame

    Parameters
    ----------

    coords_src : list
        A list of coord as (longitude, latitude) , like :
             list_coords = [(21.3224, 45.2358),
                            (21.3856, 42.0094),
                            (20.9574, 41.5286)] (coords have to be float)
    coords_dest : list, optional
        A list of coord as (longitude, latitude) , like :
             list_coords = [(21.3224, 45.2358),
                            (21.3856, 42.0094),
                            (20.9574, 41.5286)] (coords have to be float)
    ids_origin : list, optional
        A list of name/id to use to label the source axis of
        the result `DataFrame` (default: None).
    ids_dest : list, optional
        A list of name/id to use to label the destination axis of
        the result `DataFrame` (default: None).
    output : str, optional
            The type of annotated matrice to return (DataFrame or numpy array)
                'raw' for the (parsed) json response from OSRM
                'pandas', 'df' or 'DataFrame' for a DataFrame
                'numpy', 'array' or 'np' for a numpy array (default is "np")
    annotations : str, optional
        Either 'duration' (default) or 'distance'
    url_config: osrm.RequestConfig, optional
        Parameters regarding the host, version and profile to use


    Returns
    -------
        - if output=='raw' : a dict, the parsed json response.
        - if output=='np' : a numpy.ndarray containing the time in minutes,
                            a list of snapped origin coordinates,
                            a list of snapped destination coordinates.
        - if output=='pandas' : a labeled DataFrame containing the time matrix in minutes,
                                a list of snapped origin coordinates,
                                a list of snapped destination coordinates.
    """
    if output.lower() in ("numpy", "array", "np"):
        output = 1
    elif output.lower() in ("pandas", "dataframe", "df"):
        output = 2
    else:
        output = 3

    host = check_host(url_config.host)
    url = "".join([host, "/table/", url_config.version, "/", url_config.profile, "/"])

    if not send_as_polyline:
        if not coords_dest:
            url = "".join(
                [
                    url,
                    ";".join(
                        [
                            ",".join([str(coord[0]), str(coord[1])])
                            for coord in coords_src
                        ]
                    ),
                    "?annotations={}".format(annotations),
                ]
            )
        else:
            src_end = len(coords_src)
            dest_end = src_end + len(coords_dest)
            url = "".join(
                [
                    url,
                    ";".join(
                        [
                            ",".join([str(coord[0]), str(coord[1])])
                            for coord in _chain(coords_src, coords_dest)
                        ]
                    ),
                    "?sources=",
                    ";".join([str(i) for i in range(src_end)]),
                    "&destinations=",
                    ";".join([str(j) for j in range(src_end, dest_end)]),
                    "&annotations={}".format(annotations),
                ]
            )
    else:
        if not coords_dest:
            url = "".join(
                [
                    url,
                    "polyline(",
                    quote(polyline_encode([(c[1], c[0]) for c in coords_src])),
                    ")",
                    "?annotations={}".format(annotations),
                ]
            )
        else:
            src_end = len(coords_src)
            dest_end = src_end + len(coords_dest)
            url = "".join(
                [
                    url,
                    "polyline(",
                    quote(
                        polyline_encode(
                            [(c[1], c[0]) for c in _chain(coords_src, coords_dest)]
                        )
                    ),
                    ")",
                    "?sources=",
                    ";".join([str(i) for i in range(src_end)]),
                    "&destinations=",
                    ";".join([str(j) for j in range(src_end, dest_end)]),
                    "&annotations={}".format(annotations),
                ]
            )

    req = Request(url)
    if url_config.auth:
        req.add_header("Authorization", url_config.auth)
    rep = urlopen(req)
    parsed_json = json.loads(rep.read().decode("utf-8"))

    if "code" not in parsed_json or "Ok" not in parsed_json["code"]:
        raise ValueError("No distance table return by OSRM instance")

    elif output == 3:
        return parsed_json

    else:
        annoted = np.array(parsed_json["{}s".format(annotations)], dtype=float)

        new_src_coords = [ft["location"] for ft in parsed_json["sources"]]
        new_dest_coords = (
            None
            if not coords_dest
            else [ft["location"] for ft in parsed_json["destinations"]]
        )

        if (
            minutes and annotations == "duration"
        ):  # Conversion in minutes with 2 decimals:
            annoted = np.around((annoted / 60), 2)

        if output == 2:
            if not ids_origin:
                ids_origin = [i for i in range(len(coords_src))]
            if not ids_dest:
                ids_dest = (
                    ids_origin
                    if not coords_dest
                    else [i for i in range(len(coords_dest))]
                )

            annoted = DataFrame(
                annoted, index=ids_origin, columns=ids_dest, dtype=float
            )

        return annoted, new_src_coords, new_dest_coords
