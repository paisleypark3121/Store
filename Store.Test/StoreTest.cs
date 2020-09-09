using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Store.Test
{
    [TestClass]
    public class StoreTest
    {
        [TestMethod]
        public void DeleteAndInsertDevelopmentPkAndRkTest()
        {
            #region arrange
            bool expected_del = true;
            bool expected_set = true;
            string[] store_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "myPK",
                "myRK"
            };

            string myPK = "131313";
            string myRK = "242424";

            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"PartitionKey", myPK },
                {"RowKey",myRK},
                {"MyFirstElement",11 },
                {"MySecondElement",22 }
            };

            Dictionary<string, object> filter_del = new Dictionary<string, object>()
            {
                {"PartitionKey", myPK },
                {"RowKey",myRK},
            };
            #endregion

            #region delete and then insert
            IStore store = new TableStorageStore(store_args);
            bool actual_del=store.del(filter_del);
            bool actual_set = store.set(entry);
            #endregion

            #region assert
            Assert.AreEqual(expected_del, actual_del);
            Assert.AreEqual(expected_set, actual_set);
            #endregion
        }

        [TestMethod]
        public void DeleteInsertGetDevelopmentTest()
        {
            #region arrange
            bool expected_del = true;
            bool expected_set = true;
            string[] store_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "myPK",
                "myRK"
            };

            string myPK = "131313";
            string myRK = "242424";

            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"PartitionKey", myPK },
                {"RowKey",myRK},
                {"MyFirstElement",11 },
                {"MySecondElement",22 }
            };

            Dictionary<string, object> filter = new Dictionary<string, object>()
            {
                {"PartitionKey", myPK },
                {"RowKey",myRK},
            };
            #endregion

            #region delete and then insert
            IStore store = new TableStorageStore(store_args);
            bool actual_del = store.del(filter);
            bool actual_set = store.set(entry);
            object element=store.get(filter);
            Dictionary<string,object> actual=store.ToDictionary(element);
            #endregion

            #region assert
            Assert.AreEqual(expected_del, actual_del);
            Assert.AreEqual(expected_set, actual_set);
            Assert.AreEqual(entry["PartitionKey"],actual["myPK"]);
            Assert.AreEqual(entry["RowKey"], actual["myRK"]);
            Assert.AreEqual(entry["MyFirstElement"], actual["MyFirstElement"]);
            Assert.AreEqual(entry["MySecondElement"], actual["MySecondElement"]);
            #endregion
        }

        [TestMethod]
        public void InsertGetDeleteExplicitLiveTest()
        {
            #region arrange
            bool expected = true;
            string[] store_args = new string[4] {
                "DefaultEndpointsProtocol=https;AccountName=0100lanciostorage;AccountKey=KIZ/Su4EvRebOilwawZQkTWlXs6gKDO76S4uXD1q0ss7GNg5f7DC66i39Ln2B/rl/mjPjSYtigZDnOsWKDrRSg==;EndpointSuffix=core.windows.net",
                "TokenTableTest",
                "TokenID",
                "TokenValue"
            };

            string partitionKey = "TokenID";
            string rowKey = "12345678";

            Dictionary<string, object> filter_get = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
                {"RowKey",rowKey},
            };

            Dictionary<string, object> filter_del = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
            };
            #endregion

            #region insert
            IStore store = new TableStorageStore(store_args);
            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
                {"TokenValue",rowKey},
                {"ValidUntil",DateTime.Now.AddDays(10)},
            };
            bool actual = store.set(entry);

            Assert.AreEqual(expected, actual);
            #endregion

            #region get

            object element = store.get(filter_get);

            Assert.IsNotNull(element);
            Assert.IsInstanceOfType(element, typeof(Dictionary<string, object>));
            Dictionary<string, object> actualElement = (Dictionary<string, object>)element;
            Assert.IsTrue(actualElement.ContainsKey("TokenID"));
            Assert.IsTrue(actualElement.ContainsKey("TokenValue"));
            Assert.IsTrue(actualElement.ContainsKey("ValidUntil"));
            Assert.AreEqual(entry["TokenID"], actualElement["TokenID"]);
            Assert.AreEqual(entry["TokenValue"], actualElement["TokenValue"]);
            //Assert.AreEqual(entry["ValidUntil"], actualElement["ValidUntil"]);
            #endregion

            #region delete
            actual = store.del(filter_del);

            Assert.AreEqual(expected, actual);
            #endregion
        }

        [TestMethod]
        public void InsertGetDeleteEffectiveLiveTest()
        {
            #region arrange
            bool expected = true;
            string[] store_args = new string[4] {
                "DefaultEndpointsProtocol=https;AccountName=0100lanciostorage;AccountKey=KIZ/Su4EvRebOilwawZQkTWlXs6gKDO76S4uXD1q0ss7GNg5f7DC66i39Ln2B/rl/mjPjSYtigZDnOsWKDrRSg==;EndpointSuffix=core.windows.net",
                "TokenTableTest",
                "TokenID",
                "TokenValue"
            };

            string partitionKey = "TokenID";
            string rowKey = "12345678";

            Dictionary<string, object> filter_get = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
                {"TokenValue",rowKey},
            };

            Dictionary<string, object> filter_del = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
            };
            #endregion

            #region insert
            IStore store = new TableStorageStore(store_args);
            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
                {"TokenValue",rowKey},
                {"ValidUntil",DateTime.Now.AddDays(10)},
            };
            bool actual = store.set(entry);

            Assert.AreEqual(expected, actual);
            #endregion

            #region get

            object element = store.get(filter_get);

            Assert.IsNotNull(element);
            Assert.IsInstanceOfType(element, typeof(Dictionary<string, object>));
            Dictionary<string, object> actualElement = (Dictionary<string, object>)element;
            Assert.IsTrue(actualElement.ContainsKey("TokenID"));
            Assert.IsTrue(actualElement.ContainsKey("TokenValue"));
            Assert.IsTrue(actualElement.ContainsKey("ValidUntil"));
            Assert.AreEqual(entry["TokenID"], actualElement["TokenID"]);
            Assert.AreEqual(entry["TokenValue"], actualElement["TokenValue"]);
            //Assert.AreEqual(entry["ValidUntil"], actualElement["ValidUntil"]);
            #endregion

            #region delete
            actual = store.del(filter_del);

            Assert.AreEqual(expected, actual);
            #endregion
        }

        [TestMethod]
        public void Migrate2CosmosInsertGetDeleteDevelopmentTest()
        {
            #region arrange
            bool expected = true;
            string[] store_args = new string[4] {
                "UseDevelopmentStorage=true",
                "MigrateTokenTable",
                "TokenID",
                "TokenValue"
            };

            string partitionKey = "TokenID";
            string rowKey = "12345678";

            Dictionary<string, object> filter_get = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
                {"RowKey",rowKey},
            };

            Dictionary<string, object> filter_del = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
            };
            #endregion

            #region insert
            IStore store = new TableStorageStore(store_args);
            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
                {"TokenValue",rowKey},
                {"ValidUntil",DateTime.Now.AddDays(10)},
            };
            bool actual = store.set(entry);

            Assert.AreEqual(expected, actual);
            #endregion

            #region get

            object element = store.get(filter_get);

            Assert.IsNotNull(element);
            Assert.IsInstanceOfType(element, typeof(Dictionary<string, object>));
            Dictionary<string, object> actualElement = (Dictionary<string, object>)element;
            Assert.IsTrue(actualElement.ContainsKey("TokenID"));
            Assert.IsTrue(actualElement.ContainsKey("TokenValue"));
            Assert.IsTrue(actualElement.ContainsKey("ValidUntil"));
            Assert.AreEqual(entry["TokenID"], actualElement["TokenID"]);
            Assert.AreEqual(entry["TokenValue"], actualElement["TokenValue"]);
            Assert.AreEqual(entry["ValidUntil"], actualElement["ValidUntil"]);
            #endregion

            #region delete
            actual = store.del(filter_del);

            Assert.AreEqual(expected, actual);
            #endregion
        }

        [TestMethod]
        public void Migrate2CosmosInsertGetDeleteExplicitLiveTest()
        {
            #region arrange
            bool expected = true;
            string[] store_args = new string[4] {
                "DefaultEndpointsProtocol=https;AccountName=0100lanciostorage;AccountKey=KIZ/Su4EvRebOilwawZQkTWlXs6gKDO76S4uXD1q0ss7GNg5f7DC66i39Ln2B/rl/mjPjSYtigZDnOsWKDrRSg==;EndpointSuffix=core.windows.net",
                "MigrateTokenTableTest",
                "TokenID",
                "TokenValue"
            };

            string partitionKey = "TokenID";
            string rowKey = "12345678";

            Dictionary<string, object> filter_get = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
                {"RowKey",rowKey},
            };

            Dictionary<string, object> filter_del = new Dictionary<string, object>()
            {
                {"PartitionKey", partitionKey },
            };
            #endregion

            #region insert
            IStore store = new TableStorageStore(store_args);
            Dictionary<string, object> entry = new Dictionary<string, object>()
            {
                {"TokenID", partitionKey },
                {"TokenValue",rowKey},
                {"ValidUntil",DateTime.Now.AddDays(10)},
            };
            bool actual = store.set(entry);

            Assert.AreEqual(expected, actual);
            #endregion

            #region get

            object element = store.get(filter_get);

            Assert.IsNotNull(element);
            Assert.IsInstanceOfType(element, typeof(Dictionary<string, object>));
            Dictionary<string, object> actualElement = (Dictionary<string, object>)element;
            Assert.IsTrue(actualElement.ContainsKey("TokenID"));
            Assert.IsTrue(actualElement.ContainsKey("TokenValue"));
            Assert.IsTrue(actualElement.ContainsKey("ValidUntil"));
            Assert.AreEqual(entry["TokenID"], actualElement["TokenID"]);
            Assert.AreEqual(entry["TokenValue"], actualElement["TokenValue"]);
            //Assert.AreEqual(entry["ValidUntil"], actualElement["ValidUntil"]);
            #endregion

            #region delete
            actual = store.del(filter_del);

            Assert.AreEqual(expected, actual);
            #endregion
        }

        [TestMethod]
        public void GetAllFromLiveOnPKDateRangeTest()
        {
            #region arrange
            string[] store_args = new string[4] {
                "DefaultEndpointsProtocol=https;AccountName=0100lanciostorage;AccountKey=KIZ/Su4EvRebOilwawZQkTWlXs6gKDO76S4uXD1q0ss7GNg5f7DC66i39Ln2B/rl/mjPjSYtigZDnOsWKDrRSg==;EndpointSuffix=core.windows.net",
                "TestActivation",
                "Date",
                "SubscriberID"
            };

            Dictionary<string, object> filter_get = new Dictionary<string, object>()
            {
                {"fromDate", DateTime.Now.AddDays(-1) },
                {"toDate", DateTime.Now },
            };
            IStore store = new TableStorageStore(store_args);
            #endregion

            #region get
            object element = store.getAll(filter_get);
            #endregion

            #region Assert

            #endregion
        }

        [TestMethod]
        public void InvertedKeyTest()
        {
            //2518106631095405371-> - 2g
            //2518105767095405371-> - 1g
            //2518104903095405371->  NOW
            //2518104039095405371-> + 1g
            //2518103175095405371-> + 2g

            DateTime date = DateTime.Now;
            string value = Utilities.getInvertedTimeKey(date);
            value = Utilities.getInvertedTimeKey(date.Date);

        }
    }
}
