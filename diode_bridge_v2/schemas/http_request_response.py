import datetime
from ipaddress import IPv4Address
from ipaddress import IPv6Address
from typing import Dict
from typing import Optional
from typing import Union

from pydantic import AnyHttpUrl
from pydantic import BaseModel
from pydantic import Field
from pydantic import conint
from pydantic import constr
from requests import Response
from requests import request


class HttpRequest(BaseModel):
    # from fastapi
    client_ip: Union[IPv4Address, IPv6Address]  # maybe update x-forwarded-for?
    client_port: conint(ge=1, le=65535)
    http_version: constr(min_length=1, max_length=16)

    # requests things
    method: constr(regex=r'[A-Z]{1,32}')
    url: AnyHttpUrl
    params: Dict[str, str] = Field(default_factory=dict)
    data: Optional[bytes] = Field(default=None)
    json: Optional = Field(default=None)
    headers: Dict[str, str] = Field(default_factory=dict)
    cookies: Dict[str, str] = Field(default_factory=dict)
    files: Optional = Field(default=None)
    auth: Optional = Field(default=None)
    timeout: Optional[float] = Field(default=None)
    allow_redirects: Optional = Field(default=None)
    proxies: Optional = Field(default=None)
    verify: Optional[bool] = Field(default=None)
    stream: Optional = Field(default=None)
    cert: Optional = Field(default=None)

    # logging
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.now)

    def from_scope(self, scope):
        raise NotImplementedError

    def send_request(self) -> Response:
        # refer to https://stackoverflow.com/a/35974071/5340703 for multipart
        return request(method=self.method,
                       url=self.url,
                       params=self.params,
                       data=self.data,
                       json=self.json,
                       headers=self.headers,
                       cookies=self.cookies,
                       files=self.files,  # multipart/form-data
                       auth=self.auth,
                       timeout=self.timeout,
                       allow_redirects=self.allow_redirects,
                       proxies=self.proxies,
                       verify=self.verify,
                       stream=self.stream,
                       cert=self.cert,
                       )


r = requests.get('a')
