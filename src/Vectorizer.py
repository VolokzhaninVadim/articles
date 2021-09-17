###############################################################################################################################################
############################################## Импортируем необходимые модули и данные ########################################################
###############################################################################################################################################

# Для работы с параметрами
from src.Params import Params

# Для работы с БД
from src.DBResources import DBResources

# Для работы с Deep Learning
import torch

###############################################################################################################################################
############################################## Создаем объект класса ##########################################################################
class Vectorizer(object): 
    def __init__(self): 
        """
        Функция для инициализации объекта класса
        Вход:
                нет.
        Выход: 
                нет.
        """     
        self.params = Params()
        self.db_resources = DBResources()
        self.text = ' '.join(self.db_resources.lem_text()['lem_text'].values.tolist())
        vocab = set(self.text.split())
# Добавляем служебные символы
        vocab_list = self.params.special_words + list(vocab)
        word_to_ix = {w: i for i, w in enumerate(vocab_list)}
        self.n_words = len(word_to_ix)        
        self.word_to_ix = word_to_ix 
        
    def word_to_index(self, word):
        """
        Получения словаря с индексом. 
        Вход: 
                word(str) - слово. 
        Выход: 
                result(int) - индекс слова в словаре. 
        """       
# Если в словаре не находим слова, то подставляем сурогатное слово <UNK> для обозначения таких ситуаций
        try: 
            result = self.word_to_ix[word]
        except KeyError: 
            result = self.word_to_ix['<UNK>']
        return result 
    
    def line_to_tensor(self, line):
        """
        Получение из текста тензора. 
        Вход: 
            context(str) - текст для преобразования. 
        Выход: 
            (Tensor) - тензор с индексами слов из словаря. 
        """
        text = '<SOS>' + ' ' + line + ' ' + '<EOS>'
        text_list = str(text).split()
        idxs = [self.word_to_index(w) for w in text_list]   
# Если встречается меньшее количество символов, то заполняем отсутсвующим термином, иначе обрезаем по максимальную длину последовательности
        if len(idxs) < self.params.max_words: 
            idxs = idxs + [self.word_to_ix['<PAD>']] * int(self.params.max_words - len(idxs))
        else: 
            idxs = idxs[:self.params.max_words]
        return torch.tensor(idxs, dtype = torch.long)