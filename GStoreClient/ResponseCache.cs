using System;

namespace GStoreClient
{

   
    public class ResponseCache
    {

        private int _limit = 20;
        private int _currentIndex = 0;
        private int _ocupiedPositions = 0;

        //Partition,,Key,Value,Timestamp
        private Tuple<String, String, String>[] _cacheMap;

        public ResponseCache()
        {
        }

        public ResponseCache(int limit)
        {
            _cacheMap = new Tuple<String, String, String>[limit];
            _limit = limit;
        }

        public void addEntry(Tuple<String, String, String> entry)
        {
            for (int j = 0, i = _currentIndex - 1; j < _ocupiedPositions; j++, i = (i - 1) % _limit)
            {
                if (_cacheMap[i].Item1 == entry.Item1 && _cacheMap[i].Item2 == entry.Item2)
                {
                    _cacheMap[i] = entry;
                    return;
                }
            }
            _cacheMap[_currentIndex] = entry;
            _currentIndex = (_currentIndex + 1) % _limit;
            _ocupiedPositions++;
        }
        
        public String getCorrectValue(Tuple<String, String> obj)
        {
            for (int j = 0, i = _currentIndex - 1; j < _ocupiedPositions; j++, i = (i - 1) % _limit)
            {
                if (_cacheMap[i].Item1 == obj.Item1 && _cacheMap[i].Item3 == obj.Item2)
                {
                    return _cacheMap[i].Item3;
                }
            }
            return null;
        }
    }
}

