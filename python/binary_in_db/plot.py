from io import BytesIO
import random
import matplotlib.pyplot as plt


def generate_random_floats(n):
    return tuple(random.random() for i in xrange(n))


def random_scatterplot_bytes(n=100, format='png'):
    """
    :param n: The number of random data points to plot
    :param format: format of the plot (default 'png')
    :return: bytes (string) for the file contents of the plot
    """
    plt.plot(generate_random_floats(n), generate_random_floats(n), '.')

    buff = BytesIO()
    plt.savefig(buff, format=format)
    return buff.getvalue()
