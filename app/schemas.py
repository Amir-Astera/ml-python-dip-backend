from pydantic import BaseModel, EmailStr, Field


class RegisterPayload(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    email: EmailStr
    password: str = Field(min_length=6, max_length=120)


class LoginPayload(BaseModel):
    email: EmailStr
    password: str


class TransactionPayload(BaseModel):
    title: str = Field(min_length=2, max_length=120)
    category: str
    transaction_type: str
    amount: float = Field(gt=0)
    transaction_date: str
    note: str = Field(default="", max_length=200)


class MLClassifyPayload(BaseModel):
    title: str = Field(min_length=2, max_length=120)
    amount: float = Field(gt=0)
    transaction_date: str
    note: str = Field(default="", max_length=200)


class BudgetPayload(BaseModel):
    category: str
    limit_amount: float = Field(gt=0)
