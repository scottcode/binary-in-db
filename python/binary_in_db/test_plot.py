from unittest import TestCase
import plot

class ScatterplotBytes(TestCase):
    def test_returns_pdf_bytes(self):
        bytes_data = plot.random_scatterplot_bytes(n=100, format='pdf')
        self.assertEqual(bytes_data[:4], '%PDF',
                         'plot file header not of PDF type')
        self.assertGreater(len(bytes_data), 1000,
                           'plot file contents were very short, unlikely a successful plot')

    def test_returns_svg_bytes(self):
        bytes_data = plot.random_scatterplot_bytes(n=100, format='svg')
        self.assertEqual(bytes_data[:5], '<?xml',
                         'plot file header not of XML type')
        self.assertTrue('<svg' in bytes_data,
                         'plot file header not of SVG type')
        self.assertGreater(len(bytes_data), 1000,
                           'plot file contents were very short, unlikely a successful plot')