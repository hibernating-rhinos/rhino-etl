namespace Rhino.Etl.Core.Infrastructure
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;

    /// <summary>
    /// A collection of <see cref="ConnectionStringSettings"/>. By default this collection
    /// returns all connection strings as defined by <see cref="ConfigurationManager.ConnectionStrings"/>.
    /// However, it allows clients to define additional connection strings.
    /// </summary>
    public class ConnectionStringCollection : ICollection<ConnectionStringSettings>
    {
        private readonly Dictionary<string, ConnectionStringSettings> _extraItems = 
            new Dictionary<string, ConnectionStringSettings>(StringComparer.OrdinalIgnoreCase);

        /// <inheritdoc />
        public int Count
        {
            get
            {
                var configurationItemCount = ConfigurationManager.ConnectionStrings.Count;
                var extraItemCount = _extraItems.Count;
                var count = configurationItemCount + extraItemCount;

                if (configurationItemCount > 0 && extraItemCount > 0)
                {
                    foreach (var key in _extraItems.Keys)
                    {
                        if (ConfigurationManager.ConnectionStrings[key] != null) count--;
                    }
                }

                return count;
            }
        }

        /// <inheritdoc />
        public bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Gets <see cref="ConnectionStringSettings"/> instance with given name.
        /// </summary>
        /// <param name="name">Name of connection string</param>
        /// <returns>The <see cref="ConnectionStringSettings"/> instance with given <paramref name="name"/>
        /// or <c>null</c> if no matching <see cref="ConnectionStringSettings"/> is found.</returns>
        public ConnectionStringSettings this[string name]
        {
            get
            {
                return _extraItems.TryGetValue(name, out var result)
                    ? result
                    : ConfigurationManager.ConnectionStrings[name];
            }
        }

        /// <inheritdoc />
        public IEnumerator<ConnectionStringSettings> GetEnumerator()
        {
            return ConfigurationManager.ConnectionStrings
                .OfType<ConnectionStringSettings>()
                .Where(c => !_extraItems.ContainsKey(c.Name))
                .Concat(_extraItems.Values)
                .GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc />
        public void Add(ConnectionStringSettings item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            if (!ContainsConfigurationItem(item))
            {
                _extraItems.Add(item.Name, item);
            }
        }

        /// <inheritdoc />
        public void Clear()
        {
            _extraItems.Clear();
            ConfigurationManager.ConnectionStrings.Clear();
        }

        /// <summary>
        /// Indicates whether a <see cref="ConnectionStringSettings"/> instance with a given
        /// name is present in this collection.
        /// </summary>
        /// <param name="name">Name of connection string.</param>
        /// <returns><c>true</c> if a <see cref="ConnectionStringSettings"/> instance with the given <paramref name="name"/>
        /// is found, or <c>false</c> otherwise.</returns>
        public bool Contains(string name)
        {
            return _extraItems.ContainsKey(name)
                || ConfigurationManager.ConnectionStrings[name] != null;
        }

        /// <inheritdoc />
        public bool Contains(ConnectionStringSettings item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return ContainsExtraItem(item) 
                   || ContainsConfigurationItem(item);
        }

        /// <inheritdoc />
        public void CopyTo(ConnectionStringSettings[] array, int arrayIndex)
        {
            var count = this.Count;

            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex + count > array.Length) throw new ArgumentException("Array has insufficient capacity", nameof(array));
            if (arrayIndex < 0 || arrayIndex >= array.Length) throw new ArgumentOutOfRangeException(nameof(arrayIndex));

            foreach (var item in this)
            {
                array[arrayIndex++] = item;
            }
        }

        /// <summary>
        /// Removes a <see cref="ConnectionStringSettings"/> instance with a given name.
        /// </summary>
        /// <param name="name">Name of connection string.</param>
        /// <returns><c>true</c> if a <see cref="ConnectionStringSettings"/> instance with the given <paramref name="name"/>
        /// was removed, or <c>false</c> otherwise.</returns>
        public bool Remove(string name)
        {
            if (_extraItems.Remove(name)) return true;

            if (ConfigurationManager.ConnectionStrings[name] != null)
            {
                ConfigurationManager.ConnectionStrings.Remove(name);
                return true;
            }
            return false;
        }

        /// <inheritdoc />
        public bool Remove(ConnectionStringSettings item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            return RemoveExtraItem(item) 
                   || RemoveConfigurationItem(item);
        }

        /// <inheritdoc />
        private bool ContainsConfigurationItem(ConnectionStringSettings item)
        {
            return Equals(ConfigurationManager.ConnectionStrings[item.Name], item);
        }

        private bool ContainsExtraItem(ConnectionStringSettings item)
        {
            return _extraItems.TryGetValue(item.Name, out var extraItem) 
                   && Equals(item, extraItem);
        }

        private bool RemoveConfigurationItem(ConnectionStringSettings item)
        {
            if (ContainsConfigurationItem(item))
            {
                ConfigurationManager.ConnectionStrings.Remove(item.Name);
                return true;
            }

            return false;
        }

        private bool RemoveExtraItem(ConnectionStringSettings item)
        {
            return _extraItems.TryGetValue(item.Name, out var extraItem)
                   && Equals(item, extraItem)
                   && _extraItems.Remove(item.Name);
        }
    }
}