using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Store
{
    public class Utilities
    {
        public static string getInvertedTimeKey(DateTime dateTime)
        {
            if ((dateTime == null) || (dateTime == DateTime.MinValue))
                return null;

            return (DateTime.MaxValue.Ticks - dateTime.Ticks).ToString();
        }

        public static T GetObject<T>(Dictionary<string, object> dict)
        {
            Type type = typeof(T);
            var obj = Activator.CreateInstance(type);

            foreach (var kv in dict)
            {
                Type t = kv.Value.GetType();

                if (type.GetProperty(kv.Key) != null)
                {
                    //if (t==typeof(DateTime))
                    //{
                    //    DateTime val = (DateTime)kv.Value;
                    //    type.GetProperty(kv.Key).SetValue(obj, val);
                    //}
                    //else
                        type.GetProperty(kv.Key).SetValue(obj, kv.Value);
                }
            }
            return (T)obj;
        }
    }
}
