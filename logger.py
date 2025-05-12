from math import log
import os, sys
import logging
from logging.handlers import RotatingFileHandler
import datetime
import functools
from pathlib import Path
from wasabi import Printer  # type: ignore[import]
from wasabi import wrap
from wasabi import color


from rich.logging import RichHandler


#https://github.com/explosion/wasabi
msg = Printer(timestamp=True)


def is_lambda():
    c1 = os.environ.get("LAMBDA_TASK_ROOT") is not None
    c2 = os.environ.get("AWS_EXECUTION_ENV") is not None
    c3 = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None
    return c1 or c2 or c3


def is_docker():
    cgroup = Path('/proc/self/cgroup')
    return cgroup.is_file() and 'docker' in cgroup.read_text()

class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def logme(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        msg.info(f'----> {f.__name__}')
        res = f(*args, **kwargs)
        msg.info(f' {f.__name__} <---')
        return res
    return wrapped

if is_lambda():
    optimal_log_level = logging.WARNING
elif is_docker():
    optimal_log_level = logging.INFO
else:
    optimal_log_level = logging.DEBUG

# 
class LLPackerLogger(object, metaclass=SingletonType):
    _logger = None

    def __new__(cls, *args, **kwargs):
        cls._logger = super(LLPackerLogger, cls).__new__(cls)
        return cls._logger
    
    def __init__(self, log_file_name, 
                        log_level="",
                        dirname='./logs/'):
                
        self._logger = logging.getLogger(log_file_name)
        self._logger.setLevel(optimal_log_level)
        if isinstance(log_level, str):
            if hasattr(logging, log_level.upper()):
                log_level = getattr(logging, log_level.upper())
                self._logger.setLevel(log_level)
        #
        self.log_level = self._logger.level
        formatter = logging.Formatter('%(asctime)s \t %(message)s')

        now = datetime.datetime.now()
        if is_lambda():
            dirname = os.path.join('/tmp', dirname)   

        if not os.path.isdir(dirname):
            os.mkdir(dirname)

        self.logger_file_name = log_file_name 
        self.log_file_path = os.path.join(dirname, f"{log_file_name}_{now.strftime('%Y-%m-%d')}.log")
        fileHandler = RotatingFileHandler(self.log_file_path, maxBytes=10*1024*1024, backupCount=5)
        fileHandler.setLevel(logging.DEBUG)
        fileHandler.setFormatter(formatter) 
        
        streamHandler = logging.StreamHandler()

        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)

        self._logger.addHandler(fileHandler)
        self._logger.addHandler(streamHandler)
        self.msg = msg

    def set_log_level(self, log_level):
        if isinstance(log_level, str):
            if hasattr(logging, log_level.upper()):
                log_level = getattr(logging, log_level.upper())
        self.log_level = log_level
        self._logger.setLevel(log_level)
        return self
    
    def get_log_level_name(self, log_level=0):
        if log_level == 0:
            log_level = self.log_level
            
        log_level_name = 'CRITICAL'
        if isinstance(log_level, int):
            for x in ['DEBUG','INFO','WARNING','ERROR','CRITICAL']:
                if log_level <= getattr(logging, x):
                    log_level_name = x
                    break
        return log_level_name
    
    def logger(self):
        return self._logger
    
    def compose_log_message(self, *args, **kwargs):  
                
        achar = kwargs.pop('achar','-')
        if kwargs.get('arrow',""):
            message = f"{achar*5}{kwargs.pop('arrow')}{achar*5}> "
        elif kwargs.get('smallarrow',""):
            message = f"{achar*2}{kwargs.pop('smallarrow')}{achar*2}> "
        elif kwargs.get('bigarrow',""):
            message = f"{achar*8}{kwargs.pop('bigarrow')}{achar*8}> "
        else:      
            message = ''            
            
        if args:
            message += ', '.join([f"{x}" for x in args if isinstance(x, str)])
            
        fg = kwargs.pop('fg',None)
        bg = kwargs.pop('bg',None)
        bold = kwargs.pop('bold',False)
        if kwargs:
            message += ', '.join([f"{k}={v}" for k,v in kwargs.items() if isinstance(v, str)])

        # Apply color        
        if fg or bg or bold:
            message_orig = message
            try:
                message = color(message, fg=fg, bg=bg, bold=bold)
            except:
                message = message_orig
                
        # Wrap message
        message = wrap(message, wrap_max=80, indent=2)
        
        
        return message  
    
    def _tmessage(self, *args, **kwargs):
        return self.compose_log_message(*args, **kwargs)

    def divider(self, *message, **kwargs):
        if kwargs.get('level',self.log_level) >= self.log_level:
            self.msg.divider(self._tmessage(*message, **kwargs))

    def success(self, *message, **kwargs):
        if kwargs.get('level',self.log_level) >= self.log_level:
            self.msg.good(self._tmessage(*message, **kwargs))    

    def good(self, *message, **kwargs):
        if kwargs.get('level',self.log_level) >= self.log_level:
            self.msg.good(self._tmessage(*message, **kwargs))     

    def ok(self, *message, **kwargs):
        if kwargs.get('level',self.log_level) >= self.log_level:
            self.msg.info(self._tmessage(*message, **kwargs))

    def fail(self, *message, **kwargs):
        if kwargs.get('level',self.log_level) >= self.log_level:
            self.msg.fail(self._tmessage(*message, **kwargs))          

    def okinfo(self, *args, **kwargs):
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.INFO:
            return 
        message = self.compose_log_message(self, *args, **kwargs)
        self.msg.info(message)

    def okwarn(self, *args, **kwargs):
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.WARN:
            return      
        message = self.compose_log_message(self, *args, **kwargs)
        self.msg.warn(message)

    def debug(self, *args, **kwargs):  
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.DEBUG:
            return                          
        caller_line_number = sys._getframe(1).f_lineno        
        caller_filename = sys._getframe(1).f_code.co_filename
        caller_filename = caller_filename.split('/')[-1] if '/' in caller_filename else caller_filename      
        caller_func_name = sys._getframe(1).f_code.co_name

        #
        prefix = f"{caller_filename}:{caller_func_name}:{caller_line_number}"
        message = self.compose_log_message(self, *args, **kwargs)
        message = f"{message}"

        self.msg.info(message)

    def info(self, *args, **kwargs):
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.INFO:
            return        
        caller_line_number = sys._getframe(1).f_lineno        
        caller_filename = sys._getframe(1).f_code.co_filename
        caller_filename = caller_filename.split('/')[-1] if '/' in caller_filename else caller_filename      
        caller_func_name = sys._getframe(1).f_code.co_name

        #
        prefix = f"{caller_filename}:{caller_func_name}:{caller_line_number}"
        message = self.compose_log_message(self, *args, **kwargs)
        message = f"{message}"

        self.msg.info(message)

    def warn(self, *args, **kwargs):
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.WARN:
            return         
        caller_line_number = sys._getframe(1).f_lineno
        caller_filename = sys._getframe(1).f_code.co_filename
        caller_filename = caller_filename.split('/')[-1] if '/' in caller_filename else caller_filename    
        caller_func_name = sys._getframe(1).f_code.co_name
        
        #
        prefix = f"{caller_filename}:{caller_func_name}:{caller_line_number}"
        message = self.compose_log_message(self, *args, **kwargs)
        message = f"{message}"

        self.msg.warn(message)

    def error(self, *args, **kwargs):
        if kwargs.get('level',100) < self.log_level or self.log_level > logging.ERROR:
            return         
        caller_line_number = sys._getframe(1).f_lineno
        caller_filename = sys._getframe(1).f_code.co_filename
        caller_filename = caller_filename.split('/')[-1] if '/' in caller_filename else caller_filename      
        caller_func_name = sys._getframe(1).f_code.co_name
        
        #
        prefix = f"{caller_filename}:{caller_func_name}:{caller_line_number}"
        message = self.compose_log_message(self, *args, **kwargs)
        message = f"{message} - {prefix}"

        self.msg.fail(message)

# Example usage:
if __name__ == "__main__":
    logger = LLPackerLogger("test.log")
