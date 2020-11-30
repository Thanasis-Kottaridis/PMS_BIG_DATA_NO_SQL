"""
    This file is used as an extension for generate,encode and decode a 4D morton code:
    4D Morton code is a unique code that created for transforming 4 integers into one
    (In out case the 4 int values are latitude, longitude, MMSI and date-time)
    At first the set of 4 int values converted from decimal numbers to binary and then
    the values are rearranged

    FOR EXAMPLE:
    (X,Y,Z,T) = (5,9,1,7) =
    TODO:- ADD MORTON DOCUMENTATION HERE FROM MAIN PAPER PAGE 38
    # CODE REFERENCE:- https://github.com/stpsomad/DynamicPCDMS/blob/master/pointcloud/morton.py
"""

###############################################################################
######################      Morton conversion in 4D      ######################
###############################################################################
from numba import jit, int32, int64


@jit(forceobj=True)
def Expand4D(x) :
    """
    Encodes the 124 bit morton code for a 31 bit number in the 4D space using
    a divide and conquer approach for separating the bits.

    Args:
        x (int): the requested 3D dimension

    Returns:
        int: 124 bit morton code in 3D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers

    """

    if x < 0 :
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")
    x &= 0x7fffffff
    x = (x ^ x << 64) & 0x7fc0000000000000003fffff
    x = (x ^ x << 32) & 0x7fc00000003ff800000007ff
    x = (x ^ x << 16) & 0x780007c0003f0000f80007c0003f
    x = (x ^ x << 8) & 0x40380700c0380700c0380700c03807
    x = (x ^ x << 4) & 0x430843084308430843084308430843
    x = (x ^ x << 2) & 0x1090909090909090909090909090909
    x = (x ^ x << 1) & 0x1111111111111111111111111111111
    return x


def EncodeMorton4D(x, y, z, t) :
    """
    Calculates the 4D morton code from the x, y, z, t dimensions

    Args:
        x (int): the x dimension of 31 bits
        y (int): the y dimension of 31 bits
        z (int): the z dimension of 31 bits
        t (int): the time dimension of 31 bits

    Returns:
        int: 124 bit morton code in 4D
    """
    return Expand4D(x) + (Expand4D(y) << 1) + (Expand4D(z) << 2) + (Expand4D(t) << 3)


def Compact4D(x) :
    """
    Decodes the 124 bit morton code into a 31 bit number in the 4D space using
    a divide and conquer approach for separating the bits.

    Args:
        x (int): a 124 bit morton code

    Returns:
        int: a dimension in 4D space

    Raises:
        Exception: ERROR: Morton code is always positive
    """
    if x < 0 :
        raise Exception("""ERROR: Morton code is always positive""")
    x &= 0x1111111111111111111111111111111
    x = (x ^ (x >> 1)) & 0x1090909090909090909090909090909
    x = (x ^ (x >> 2)) & 0x430843084308430843084308430843
    x = (x ^ (x >> 4)) & 0x40380700c0380700c0380700c03807
    x = (x ^ (x >> 8)) & 0x780007c0003f0000f80007c0003f
    x = (x ^ (x >> 16)) & 0x7fc00000003ff800000007ff
    x = (x ^ (x >> 32)) & 0x7fc0000000000000003fffff
    x = (x ^ (x >> 64)) & 0x7fffffff
    return x


def DecodeMorton4Dt(mortonCode) :
    """
    Calculates the t coordinate from a 124 bit morton code

    Args:
        mortonCode (int): the 124 bit morton code

    Returns:
        int: 31 bit t coordinate in 4D
    """
    return Compact4D(mortonCode >> 3)


def DecodeMorton4DX(mortonCode) :
    """
    Calculates the x coordinate from a 124 bit morton code

    Args:
        mortonCode (int): the 124 bit morton code

    Returns:
        int: 31 bit x coordinate in 4D
    """
    return Compact4D(mortonCode)


def DecodeMorton4DY(mortonCode) :
    """
    Calculates the y coordinate from a 124 bit morton code

    Args:
        mortonCode (int): the 124 bit morton code

    Returns:
        int: 31 bit y coordinate in 4D
    """
    return Compact4D(mortonCode >> 1)


def DecodeMorton4DZ(mortonCode) :
    """
    Calculates the z coordinate from a 124 bit morton code

    Args:
        mortonCode (int): the 124 bit morton code

    Returns:
        int: 31 bit z coordinate in 4D
    """
    return Compact4D(mortonCode >> 2)


def lonLatToInt(x, y):
    """
        helper func for converting lat and lon into int:
        by definition the valid range of Latitude is:
                Latitude : max/min +90 to -90

        And the valid range of Longitude is:
                Longitude : max/min +180 to -180

        So we have to convert x = lon and y = lat into
        positive integers by adding them the appropriate
        value and then multiply them by 1.000.000 in order
        to make them integers and preserve the 7 decimal
        accuracy of the coordinates
    """
    x = int((x + 180) * 10000000)
    y = int((y + 90) * 10000000)
    return x, y


def intToLon(val):
    """
        Perform reverse transformation in order to retrieve
        longitude from integer by dividing the number with
        1.000.000 and then subtract 180
        TODO:- CHECK IF ROUNDING NEEDS TO APPLY HERE ON 7TH DECIMAL

    """

    return (val / 10000000) - 180


def intToLat(val):
    """
        Perform reverse transformation in order to retrieve
        latitude from integer by dividing the number with
        1.000.000 and then subtract 90
        TODO:- CHECK IF ROUNDING NEEDS TO APPLY HERE ON 7TH DECIMAL
    """

    return (val / 10000000) - 90


if __name__ == '__main__':
    # x = long y = lat
    lon, lat = lonLatToInt(-4.4774585, 48.21438)
    morton = EncodeMorton4D(lon, lat, 37100300, 1444177613)
    print(morton)
    print("X", intToLon(DecodeMorton4DX(morton)))
    print("Y", intToLat(DecodeMorton4DY(morton)))
    print("Z", DecodeMorton4DZ(morton))
    print("T", DecodeMorton4Dt(morton))
