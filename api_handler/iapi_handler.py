from abc import ABC, abstractmethod


class IAPIHandler(ABC):
    """
    Interface of HTTP Requests (API) as part of Factory Method implementation.

    Attributes:
        None

    Methods:
        def _get_request(self): declaration of http get request method
        def url_query(self): declaration of api's url query method
    """
    @abstractmethod
    def _get_request(self) -> Exception:
        """ Declaration of http get request method

        Args:
            None
        Parameters:
            None
        Raises:
            NotImplementedError: raise exception when the method has been called and it's not implemented.

        """
        raise NotImplementedError

    @abstractmethod
    def url_query(self) -> Exception:
        """ Declaration of api's url query method

        Args:
            None
        Parameters:
            None
        Returns:
            NotImplementedError Exception

        """
        raise NotImplementedError

