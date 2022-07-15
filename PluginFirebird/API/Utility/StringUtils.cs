using System;
using System.Text;

namespace PluginFirebird.API.Utility
{
    public static class StringUtils
    {
        public static string ToAllCaps(this string str)
        {
            var allCapsBuilder = new StringBuilder();
            
            str.ForEach(c => allCapsBuilder.Append(c.ToString().ToUpper()));

            return allCapsBuilder.ToString();
        }
        
        public static void ForEach(this string s, Action<char> loopAction)
        {
            foreach (var c in s)
            {
                loopAction(c);
            }
        }

        public static bool IsOracleEscaped(this string s)
        {
            return s.Trim().StartsWith('\"');
        }
    }
}