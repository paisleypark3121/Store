using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Web;

namespace Store
{
    public class TableStorageEntry : TableEntity
    {
        private IDictionary<string, EntityProperty> properties;

        public TableStorageEntry(
            string _partitionKey,
            string _rowKey,
            Dictionary<string,object> o)
        {
            #region preconditions
            if (o == null)
                throw new Exception("Invalid object");
            if (!o.ContainsKey(_partitionKey) && !o.ContainsKey("PartitionKey"))
                throw new Exception("Missing partitionKey " + _partitionKey + " in object");
            #endregion

            #region rowKey preconditions
            string rowkey = null;
            if (string.IsNullOrEmpty(_rowKey))
            {
                rowkey = Utilities.getInvertedTimeKey(DateTime.Now).ToString();
                this.RowKey = HttpUtility.UrlEncode(rowkey);
            } else if ((!o.ContainsKey(_rowKey) && !o.ContainsKey("RowKey")))
                throw new Exception("Missing rowKey " + _rowKey + " in object");
            #endregion

            #region timestamp
            this.Timestamp = DateTime.Now;
            properties = new Dictionary<string, EntityProperty>();
            #endregion

            #region properties
            foreach (string key in o.Keys)
            {
                #region property: name - value
                object value = o[key];
                if (value == null)
                    continue;
                string name = key;
                #endregion

                #region PartitionKey
                if ((key == _partitionKey) || (key== "PartitionKey"))
                {
                    name = "PartitionKey";
                    this.PartitionKey = HttpUtility.UrlEncode(value.ToString());
                    continue;
                }
                #endregion

                #region RowKey
                if ((key == _rowKey) || (key== "RowKey"))
                {
                    name = "RowKey";
                    this.RowKey = HttpUtility.UrlEncode(value.ToString());
                    continue;
                }
                #endregion

                if (value.GetType() == typeof(string))
                    value = HttpUtility.UrlEncode(value.ToString());
                AddValue(name, value, properties);
            }
            #endregion
        }

        //public int VerbosityLevel { get; set;}
        public TableStorageEntry() { }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return properties;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> _properties, OperationContext operationContext)
        {
            this.properties = _properties;
        }

        internal void AddValue(string name, object value, IDictionary<string, EntityProperty> properties)
        {
            if (value.GetType() == typeof(string))
                properties.Add(name, new EntityProperty((string)value));
            if (value.GetType() == typeof(int))
                properties.Add(name, new EntityProperty((int)value));
            if (value.GetType() == typeof(DateTime))
                properties.Add(name, new EntityProperty((DateTime)value));
            if (value.GetType() == typeof(bool))
                properties.Add(name, new EntityProperty((bool)value));
            if (value.GetType() == typeof(double))
                properties.Add(name, new EntityProperty((double)value));
            if (value.GetType() == typeof(short))
                properties.Add(name, new EntityProperty((short)value));
            if (value.GetType() == typeof(float))
                properties.Add(name, new EntityProperty((float)value));
            if (value.GetType() == typeof(char))
                properties.Add(name, new EntityProperty((char)value));
            if (value.GetType() == typeof(byte))
                properties.Add(name, new EntityProperty((byte)value));
            if (value.GetType() == typeof(long))
                properties.Add(name, new EntityProperty((long)value));
        }

        public Dictionary<string,object> ToDictionary(
            string PartitionKey,
            string RowKey)
        {
            #region try
            try
            {
                Dictionary<string, object> entity = new Dictionary<string, object>();

                #region preconditions
                if (string.IsNullOrEmpty(PartitionKey))
                    throw new Exception("Missing mandatory parameter PartitionKey");
                #endregion

                entity.Add(PartitionKey, this.PartitionKey);
                if (string.IsNullOrEmpty(RowKey))
                    entity.Add("RowKey", this.RowKey);
                else
                    entity.Add(RowKey, this.RowKey);

                foreach (var entry in properties)
                {
                    EdmType _type = ((EntityProperty)entry.Value).PropertyType;

                    if (_type == EdmType.String)
                        entity.Add(entry.Key, entry.Value.StringValue);
                    else if (_type == EdmType.Int32)
                        entity.Add(entry.Key, entry.Value.Int32Value);
                    else if (_type == EdmType.Int64)
                        entity.Add(entry.Key, entry.Value.Int64Value);
                    else if (_type == EdmType.DateTime)
                        entity.Add(entry.Key, entry.Value.DateTime);
                    else if (_type == EdmType.Boolean)
                        entity.Add(entry.Key, entry.Value.BooleanValue);
                    else if (_type == EdmType.Double)
                        entity.Add(entry.Key, entry.Value.DoubleValue);
                }

                return entity;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return null;
            }
            #endregion
        }
    }
}
