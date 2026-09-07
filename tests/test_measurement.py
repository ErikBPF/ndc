import tempfile
from pathlib import Path
import unittest
from test_harness import module


class MeasurementTests(unittest.TestCase):
    def test_short_window_is_unavailable_not_zero(self):
        merge = module('merge_monitor')
        rows = [[0,0,1,0,0], [1,100,2,10,20], [2,200,3,20,40]]
        self.assertIsNone(merge.window(rows,1.1,1.2))

    def test_tree_rss_includes_children(self):
        monitor = module('monitor')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for pid,parent,rss in [(10,1,100),(11,10,200)]:
                d=root/str(pid);d.mkdir()
                fields=['S',str(parent)]+['0']*30
                fields[11]='5';fields[12]='3';fields[19]=str(pid)
                (d/'stat').write_text(f'{pid} (worker name) '+ ' '.join(fields))
                (d/'status').write_text(f'VmRSS:\t{rss} kB\n')
                (d/'io').write_text('read_bytes: 10\nwrite_bytes: 20\n')
            self.assertTrue(hasattr(monitor,'sample'), 'tree sampler missing')
            result=monitor.sample(10,root)
            self.assertEqual(result['rss_kb'],300)
            self.assertEqual(len(result['processes']),2)


if __name__ == '__main__':
    unittest.main()
