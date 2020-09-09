
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Store
{
    public class TableStorageStore : IStore
    {
        #region variables
        private string connectionString = null;
        private string resourceName = null;
        private string partitionKey = null;
        private string rowKey = null;
        #endregion

        #region storage management
        private CloudStorageAccount storageAccount = null;
        private CloudTableClient tableClient = null;
        private CloudTable table = null;
        #endregion

        public TableStorageStore() { }

        /// <summary>
        /// It is mandatory the PartitionKey
        /// In case of missing RowKey it will be used the inverted timestamp
        /// </summary>
        public TableStorageStore(string[] args)
        {
            #region preconditions
            if ((args == null) ||
                (args.Length < 3))
                throw new Exception("Missing mandatory parameters args");
            #endregion

            #region Manage variables
            connectionString = args[0].ToString();
            resourceName = args[1].ToString();
            partitionKey = args[2].ToString();
            if (args.Length == 4)
                rowKey = args[3].ToString();
            #endregion

            #region preconditions
            if (string.IsNullOrEmpty(connectionString) ||
                string.IsNullOrEmpty(resourceName) ||
                string.IsNullOrEmpty(partitionKey))
                throw new Exception("Missing mandatory parameters");
            #endregion

            #region manage table
            storageAccount = CloudStorageAccount.Parse(connectionString);
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(resourceName);
            table.CreateIfNotExists();
            #endregion
        }

        /// <summary>
        /// It returns the TableStorageEntry object
        /// In case there are "labels" for PartitionKey and RowKey, those will be returned
        /// </summary>
        public object get(Dictionary<string, object> filter)
        {
            #region try
            try
            {
                #region preconditions
                if (filter==null)
                    throw new Exception("Invalid parameters");
                #endregion

                #region only partitionKey
                if (filter.Count == 1)
                {
                    string pk = null;
                    if (filter.ContainsKey("PartitionKey"))
                        pk = filter["PartitionKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(partitionKey))
                            pk = filter[partitionKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    return Get(pk);
                }
                #endregion

                #region partitionKey and rowKey
                if (filter.Count==2)
                {
                    string pk = null;
                    if (filter.ContainsKey("PartitionKey"))
                        pk = filter["PartitionKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(partitionKey))
                            pk = filter[partitionKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    string rk = null;
                    if (filter.ContainsKey("RowKey"))
                        rk = filter["RowKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(rowKey))
                            rk = filter[rowKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    return Get(pk, rk);
                    //TableStorageEntry _entry = Get(pk, rk);
                    //if (_entry == null)
                    //    return null;

                    //return _entry.ToDictionary(partitionKey, rowKey);
                }
                #endregion

                #region else
                else
                    throw new Exception("Invalid filter length");
                #endregion
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        /// <summary>
        /// It performs the insert into the TableStorage; if the element already exists it returns false
        /// The element dictionary can be written in the form {"PartitionKey",pkvalue},{"RowKey",rkvalue}
        /// or using explicitely the name for the partitionkey and rowkey
        /// </summary>
        public bool set(Dictionary<string, object> element)
        {
            #region try
            try
            {
                TableStorageEntry table_entry = new TableStorageEntry(partitionKey, rowKey, element);
                TableOperation insertOperation = TableOperation.Insert(table_entry);
                table.Execute(insertOperation);

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return false;
            }
            #endregion
        }

        /// <summary>
        /// It performs the update into the TableStorage
        /// The element dictionary can be written in the form {"PartitionKey",pkvalue},{"RowKey",rkvalue}
        /// or using explicitely the name for the partitionkey and rowkey
        /// </summary>
        public bool update(Dictionary<string, object> element)
        {
            #region try
            try
            {
                TableStorageEntry table_entry = new TableStorageEntry(partitionKey, rowKey, element);
                TableOperation insertOperation = TableOperation.InsertOrMerge(table_entry);
                table.Execute(insertOperation);

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return false;
            }
            #endregion
        }

        /// <summary>
        /// It performs the delete from the TableStorage of the element with given PK (and RK)
        /// The filter dictionary can be written in the form {"PartitionKey",pkvalue},{"RowKey",rkvalue}
        /// or using explicitely the name for the partitionkey and rowkey
        /// </summary>
        public bool del(Dictionary<string, object> filter)
        {
            #region try
            try
            {
                #region preconditions
                if (filter == null)
                    throw new Exception("Invalid parameters");
                #endregion

                #region only partitionKey
                if (filter.Count == 1)
                {
                    string pk = null;
                    if (filter.ContainsKey("PartitionKey"))
                        pk = filter["PartitionKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(partitionKey))
                            pk = filter[partitionKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    return Delete(pk);
                }
                #endregion

                #region partitionKey and rowKey
                if (filter.Count == 2)
                {
                    string pk = null;
                    if (filter.ContainsKey("PartitionKey"))
                        pk = filter["PartitionKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(partitionKey))
                            pk = filter[partitionKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    string rk = null;
                    if (filter.ContainsKey("RowKey"))
                        rk = filter["RowKey"].ToString();
                    else
                    {
                        if (filter.ContainsKey(rowKey))
                            rk = filter[rowKey].ToString();
                        else
                            throw new Exception("Invalid filter");
                    }

                    return Delete(pk, rk);
                }
                #endregion

                #region else
                else
                    throw new Exception("Invalid filter length");
                #endregion
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return false;
            }
            #endregion
        }

        /// <summary>
        /// filter must be in the form:
        /// 1. only PartitionKey
        /// 2. data range on RowKey (with given PartitionKey)
        /// 3. data range on PartitionKey (with given RowKey)
        /// 4. data range on PartitionKey (without given RowKey)
        /// The filter dictionary can be written in the form {"PartitionKey",pkvalue},{"RowKey",rkvalue}
        /// or using explicitely the name for the partitionkey and rowkey
        /// The data range must be in the form: fromDate / toDate
        /// </summary>
        /// <returns></returns>
        public object getAll(Dictionary<string, object> filter)
        {
            #region try
            try
            {
                #region preconditions
                if (filter == null)
                    return GetAll();
                #endregion

                #region only partitionKey
                if ((filter.Count == 1) && 
                    (filter.ContainsKey("PartitionKey") || (filter.ContainsKey(partitionKey)))
                    )
                {
                    string pk = null;
                    if (filter.ContainsKey("PartitionKey"))
                        pk = filter["PartitionKey"].ToString();
                    else
                        pk = filter[partitionKey].ToString();

                    if (string.IsNullOrEmpty(pk))
                        throw new Exception("Error in retrieving partitionKey");

                    return GetAll(pk);
                }
                #endregion

                #region date range on rowkey (given a partitionKey)
                else if (filter.ContainsKey("PartitionKey"))
                {
                    #region partitionKey and fromDate
                    if (filter.Count == 2)
                    {
                        string pk = null;
                        if (filter.ContainsKey("PartitionKey"))
                            pk = filter["PartitionKey"].ToString();
                        else
                            pk = filter[partitionKey].ToString();

                        if (string.IsNullOrEmpty(pk))
                            throw new Exception("Error in retrieving partitionKey");

                        DateTime fromDate = DateTime.MaxValue;
                        if (filter.ContainsKey("fromDate"))
                            fromDate = DateTime.Parse(filter["fromDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        return GetByPk(pk, fromDate);
                    }
                    #endregion

                    #region partitionKey, fromDate and toDate
                    else if (filter.Count == 3)
                    {
                        string pk = null;
                        if (filter.ContainsKey("PartitionKey"))
                            pk = filter["PartitionKey"].ToString();
                        else
                            pk = filter[partitionKey].ToString();

                        if (string.IsNullOrEmpty(pk))
                            throw new Exception("Error in retrieving partitionKey");

                        DateTime fromDate = DateTime.MaxValue;
                        if (filter.ContainsKey("fromDate"))
                            fromDate = DateTime.Parse(filter["fromDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        DateTime toDate = DateTime.MaxValue;
                        if (filter.ContainsKey("toDate"))
                            toDate = DateTime.Parse(filter["toDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        return GetByPk(pk, fromDate, toDate);
                    }
                    #endregion

                    #region else
                    else
                        throw new Exception("Invalid filter length");
                    #endregion
                }
                #endregion

                #region date range on partitionkey (given a rowKey)
                else if (filter.ContainsKey("RowKey") || filter.ContainsKey(rowKey))
                {
                    #region rowKey and fromDate
                    if (filter.Count == 2)
                    {
                        string rk = null;
                        if (filter.ContainsKey("RowKey"))
                            rk = filter["RowKey"].ToString();
                        else
                            rk = filter[rowKey].ToString();

                        if (string.IsNullOrEmpty(rk))
                            throw new Exception("Error in retrieving rowKey");

                        DateTime fromDate = DateTime.MaxValue;
                        if (filter.ContainsKey("fromDate"))
                            fromDate = DateTime.Parse(filter["fromDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        return GetByRk(rk, fromDate);
                    }
                    #endregion

                    #region rowKey, fromDate and toDate
                    else if (filter.Count == 3)
                    {
                        string rk = null;
                        if (filter.ContainsKey("RowKey"))
                            rk = filter["RowKey"].ToString();
                        else
                            rk = filter[rowKey].ToString();

                        if (string.IsNullOrEmpty(rk))
                            throw new Exception("Error in retrieving rowKey");

                        DateTime fromDate = DateTime.MaxValue;
                        if (filter.ContainsKey("fromDate"))
                            fromDate = DateTime.Parse(filter["fromDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        DateTime toDate = DateTime.MaxValue;
                        if (filter.ContainsKey("toDate"))
                            toDate = DateTime.Parse(filter["toDate"].ToString());
                        else
                            throw new Exception("Invalid filter");

                        return GetByRk(rk, fromDate, toDate);
                    }
                    #endregion

                    #region else
                    else
                        throw new Exception("Invalid filter length");
                    #endregion
                }
                #endregion

                #region date range on partitionkey (without a rowKey)
                else if (filter.ContainsKey("fromDate"))
                {
                    #region only fromDate
                    if (filter.Count == 1)
                    {
                        DateTime fromDate = DateTime.Parse(filter["fromDate"].ToString());

                        return GetByPk(fromDate);
                    }
                    #endregion

                    #region fromDate and toDate
                    else if (filter.Count == 2)
                    {
                        DateTime fromDate = DateTime.Parse(filter["fromDate"].ToString());
                        DateTime toDate = DateTime.Parse(filter["toDate"].ToString());

                        return GetByPk(fromDate, toDate);
                    }
                    #endregion

                    #region else
                    else
                        throw new Exception("Invalid filter length");
                    #endregion
                }
                #endregion

                #region else
                else
                    throw new Exception("Invalid filter length");
                #endregion
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        /// <summary>
        /// Convert the TableStorageEntry x in the object of type T
        /// </summary>
        public T convert<T>(object x)
        {
            TableStorageEntry entry = (TableStorageEntry)x;
            T elem = Utilities.GetObject<T>(entry.ToDictionary(partitionKey, rowKey));
            return elem;
        }

        /// <summary>
        /// Convert the List of TableStorageEntry in the list of objects of type T
        /// </summary>
        public List<T> convertList<T>(object x)
        {
            List<TableStorageEntry> listT = (List<TableStorageEntry>)x;

            List<T> list = new List<T>();
            foreach (TableStorageEntry entry in listT)
            {
                T elem = Utilities.GetObject<T>(entry.ToDictionary(partitionKey, rowKey));
                list.Add(elem);
            }

            return list;
        }

        #region utilities
        public TableStorageEntry Get(string pk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk));

                TableStorageEntry _entry = table.ExecuteQuery(rangeQuery).FirstOrDefault<TableStorageEntry>();
                if (_entry == null)
                    return null;

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetAll()
        {
            #region try
            try
            {
                List<TableStorageEntry> entries = table.ExecuteQuery(new TableQuery<TableStorageEntry>()).ToList<TableStorageEntry>();
                if (entries == null)
                    return null;

                return entries;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetAll(string pk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk));

                List<TableStorageEntry> entries = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();
                if (entries == null)
                    return null;

                return entries;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public TableStorageEntry Get(string pk, string rk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk) || string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk)));

                TableStorageEntry _entry = table.ExecuteQuery(rangeQuery).FirstOrDefault<TableStorageEntry>();
                if (_entry == null)
                    return null;

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByPk(string pk, DateTime date)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endRowKey = (DateTime.MaxValue.Ticks - date.Date.Ticks).ToString();
                string startRowKey = (DateTime.MaxValue.Ticks - DateTime.Now.Ticks).ToString();

                string partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
                string rowUpFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey);
                string rowDownFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startRowKey);

                string filter = TableQuery.CombineFilters(rowDownFilter, TableOperators.And, rowUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, partitionKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByRk(string rk, DateTime date)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endPrimaryKey = (DateTime.MaxValue.Ticks - date.Date.Ticks).ToString();
                string startPrimaryKey = (DateTime.MaxValue.Ticks - DateTime.Now.Ticks).ToString();

                string rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk);
                string partitionUpFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPrimaryKey);
                string partitionDownFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPrimaryKey);

                string filter = TableQuery.CombineFilters(partitionDownFilter, TableOperators.And, partitionUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByPk(string pk, DateTime fromDate, DateTime toDate)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endRowKey = (DateTime.MaxValue.Ticks - fromDate.Ticks).ToString();
                string startRowKey = (DateTime.MaxValue.Ticks - toDate.Ticks).ToString();

                string partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
                string rowUpFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey);
                string rowDownFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startRowKey);

                string filter = TableQuery.CombineFilters(rowDownFilter, TableOperators.And, rowUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, partitionKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByRk(string rk, DateTime fromDate, DateTime toDate)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endPartitionKey = (DateTime.MaxValue.Ticks - fromDate.Ticks).ToString();
                string startPartitionKey = (DateTime.MaxValue.Ticks - toDate.Ticks).ToString();

                string rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk);
                string partitionUpFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPartitionKey);
                string partitionDownFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPartitionKey);

                string filter = TableQuery.CombineFilters(partitionDownFilter, TableOperators.And, partitionUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByPk(DateTime date)
        {
            #region try
            try
            {
                string endPrimaryKey = (DateTime.MaxValue.Ticks - date.Date.Ticks).ToString();
                string startPrimaryKey = (DateTime.MaxValue.Ticks - DateTime.Now.Ticks).ToString();

                string partitionUpFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPrimaryKey);
                string partitionDownFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPrimaryKey);

                string filter = TableQuery.CombineFilters(partitionDownFilter, TableOperators.And, partitionUpFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> GetByPk(DateTime fromDate, DateTime toDate)
        {
            #region try
            try
            {
                string endPartitionKey = (DateTime.MaxValue.Ticks - fromDate.Ticks).ToString();
                string startPartitionKey = (DateTime.MaxValue.Ticks - toDate.Ticks).ToString();

                string partitionUpFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPartitionKey);
                string partitionDownFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPartitionKey);

                string filter = TableQuery.CombineFilters(partitionDownFilter, TableOperators.And, partitionUpFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public bool Delete(string pk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk));

                TableStorageEntry _entry = table.ExecuteQuery(rangeQuery).FirstOrDefault<TableStorageEntry>();

                if (_entry != null)
                {
                    TableOperation delete = TableOperation.Delete(_entry);
                    table.Execute(delete);
                }
                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return false;
            }
            #endregion
        }

        public bool Delete(string pk, string rk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk) || string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableOperation retrieve = TableOperation.Retrieve(pk, rk);
                TableResult result = table.Execute(retrieve);

                if (result.Result != null)
                {
                    TableOperation delete = TableOperation.Delete((ITableEntity)result.Result);
                    table.Execute(delete);
                }

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return false;
            }
            #endregion
        }

        public Dictionary<string, object> ToDictionary(object element)
        {
            if (element == null)
                return null;

            TableStorageEntry entry = (TableStorageEntry)element;
            return entry.ToDictionary(partitionKey, rowKey);
        }
        #endregion
    }
}
