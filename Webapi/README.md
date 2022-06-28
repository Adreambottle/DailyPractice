# README



### Problem:

* Python bottle 框架 request.params.get() 获取数据中文乱码的解决方法

```python
# Original Code
basicInfo.abstract = request.params.get('abstract', default=None, type=str)
```
* 如果使用 `request.params.get(tagName)` 会出现乱码

* 方法一：

```python
request.params.tagName
basicInfo.abstract = request.params.abstract
```

* 方法二：
```python
from bottle import request, FormsDict

@test_app.route('/add', method='POST')
def test_add():
    form = FormDict(request.params).decode('utf-8')
    ...
    basicInfo = BasicInfo()
    basicInfo.abstract = form.get('abstact', default='', type=str)
    ...
```

* 方法三：
```python
from bottle import request, FormDict

@test_app.route('/add', method='POST')
def test_add():
    basicInfo = BasicInfo()
    basicInfo.abstract = FormDict(request.params).getunicode('abstract', default='')
    ...
```



