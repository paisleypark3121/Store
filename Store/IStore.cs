using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace Store
{
    public interface IStore
    {
        /// <summary>
        /// insert element, if it already exists returns false
        /// </summary>
        bool set(Dictionary<string,object> element);
        /// <summary>
        /// insert or merge the element
        /// </summary>
        bool update(Dictionary<string, object> element);
        object get(Dictionary<string, object> filter);
        bool del(Dictionary<string, object> filter);
        object getAll(Dictionary<string, object> filter);
        T convert<T>(object element);
        List<T> convertList<T>(object element);
        Dictionary<string, object> ToDictionary(object element);
    }
}
