###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################

# Для работы с параметрами
from src.Params import Params

# Для векторизации
from src.Vectorizer import Vectorizer

# Для работы с Deep Learning
from torchvision import transforms
from torch.utils.data import Dataset

# Для мониторинга циклов
from tqdm.notebook import tqdm_notebook 

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################

# Создаем объект класса Dataset
class DataSet(Dataset):    
    def __init__(self, text_list, vectorizer):
        """
        Инициализации класса. 
        Вход: 
                text_list(list) - лист с номрализованным текстом.
                vectorizer(Vectorizer) - Vectorizer текста. 
        Выход: 
                нет. 
        """            
        self.vectorizer = vectorizer 
        self.data_set = text_list
        self.transforms_text = transforms.Compose([transforms.Lambda(lambda x: self.vectorizer.line_to_tensor(x))])
        
    def __len__(self):
        """
        Возврщения содержимого data_set. 
        Вход: 
            нет.
        Выход: 
            нет. 
        """
        return len(self.data_set)
    
    def __getitem__(self, idx):
        """
        Возвращение трансформированного элемента data set. 
        Вход: 
            idx(int) - индекс элмента.
        Выход: 
            (Tensor) - тензор с индексами слов из словаря. 
        """
        if self.transforms_text:
            result = self.transforms_text(self.data_set[idx])
        else: 
            result = self.data_set[idx]  
        return result