const apiUrl = 'api/app';
const app = new Vue({
  el: '#app',
  data() {
    return {
      type: 0,
      tableData: [],
      totalData: [],
      loading: false,
      listResourceInfo: '',
      paginationConfig: {
        page: 1,
        size: 10,
        total: 0,
      },
      data: {
        applicationId: '',
        arguments: {
          commandLine: '',
          executorCores: 1,
          executorMemory: 0,
          numExecutors: 0,
          queue: null,
        },
        completedContainers: [],
        containers: [],
        logUrl: '',
        numAllocatedExecutors: 0,
        numPendingExecutors: 0,
        numRunningExecutors: 0,
        numTotalExecutors: 0,
        startTime: Date.now(),
        trackingUrl: '',
      },
    };
  },
  mounted() {
    this.loading = true;
    this.fetchData();
  },
  computed: {
    resourceInfo() {
      const { executorCores, executorMemory, numExecutors, queue } = this.data.arguments;
      const str = `${numExecutors} instances (vcore: ${executorCores}, memory: ${executorMemory}MB)`;
      return str;
    },
  },
  methods: {
    //获取数据
    fetchData() {
      this.ajax({
        url: apiUrl,
        success: res => {
          this.data = JSON.parse(res);
          this.changeTab(0);
          this.loading = false;
          console.log(this.data);
        },
        fail: () => {
          this.$message.error('数据请求报错');
          this.loading = false;
        },
      });
    },
    //日志跳转
    route(url) {
      if (url) window.open(url, '_blank');
    },
    //分页 page
    changePage(page) {
      this.paginationConfig.page = page;
      this.tableData = this.filterData(this.totalData);
    },
    //分页size
    changeSize(size) {
      this.paginationConfig.size = size;
      this.tableData = this.filterData(this.totalData);
    },
    //分页数据过滤
    filterData(arr) {
      const { page, size } = this.paginationConfig;
      const from = (page - 1) * size;
      return arr.slice(from, from + size);
    },
    //列表数据状态切换
    changeTab(type) {
      this.type = type;
      this.paginationConfig.page = 1;
      this.paginationConfig.size = 10;
      let { containers, completedContainers } = this.data;
      this.totalData = !type ? containers : completedContainers;
      this.tableData = this.filterData(this.totalData);
      this.paginationConfig.total = this.totalData.length;
      this.handleResource(this.totalData);
    },
    //统计列表数据 resource 值
    handleResource(data) {
      const count = data.length;
      let cpuCount = 0,
        memCount = 0;
      data.forEach(item => {
        cpuCount += item.vcore;
        memCount += item.memory;
      });
      this.listResourceInfo = `${count} instances (Total vcore: ${cpuCount}, Total memory: ${memCount}MB)`;
    },
    //时间处理
    backtime: function (time) {
      const dt = new Date(time),
        y = dt.getFullYear(),
        m = dt.getMonth() + 1 < 10 ? '0' + (dt.getMonth() + 1) : dt.getMonth() + 1,
        d = dt.getDate() < 10 ? '0' + dt.getDate() : dt.getDate(),
        h = dt.getHours() < 10 ? '0' + dt.getHours() : dt.getHours(),
        mm = dt.getMinutes() < 10 ? '0' + dt.getMinutes() : dt.getMinutes(),
        s = dt.getSeconds() < 10 ? '0' + dt.getSeconds() : dt.getSeconds();
      return y + '-' + m + '-' + d + ' ' + h + ':' + mm + ':' + s;
    },
    //数据请求
    ajax(options) {
      options = options || {};
      options.type = (options.type || 'GET').toUpperCase();
      options.dataType = options.dataType || 'json';
      options.async = options.async || true;

      let params = options.data;
      let xhr;

      if (window.XMLHttpRequest) {
        xhr = new XMLHttpRequest();
      } else {
        xhr = new ActiveXObject('Microsoft.XMLHTTP');
      }
      xhr.onreadystatechange = function () {
        if (xhr.readyState == 4) {
          const status = xhr.status;
          if (status >= 200 && status < 300) {
            options.success && options.success(xhr.responseText, xhr.responseXML);
          } else {
            options.fail && options.fail(status);
          }
        }
      };
      if (options.type == 'GET') {
        xhr.open('GET', options.url, options.async);
        xhr.send();
      } else if (options.type == 'POST') {
        xhr.open('POST', options.url, options.async);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.send(params);
      }
    },
  },
});
