#include "textenc.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* ── Таблицы перекодировки ────────────────────────────────────────────────
 * Для каждой однобайтовой кодировки — код символа Unicode для байтов 0x80..0xFF.
 * Ниже 0x80 все три кодировки совпадают с ASCII, поэтому таблица начинается с
 * 0x80. Ноль означает «в этой кодировке байт не определён». */

static const unsigned short CP1251[128] = {
    0x0402,0x0403,0x201A,0x0453,0x201E,0x2026,0x2020,0x2021,
    0x20AC,0x2030,0x0409,0x2039,0x040A,0x040C,0x040B,0x040F,
    0x0452,0x2018,0x2019,0x201C,0x201D,0x2022,0x2013,0x2014,
    0x0000,0x2122,0x0459,0x203A,0x045A,0x045C,0x045B,0x045F,
    0x00A0,0x040E,0x045E,0x0408,0x00A4,0x0490,0x00A6,0x00A7,
    0x0401,0x00A9,0x0404,0x00AB,0x00AC,0x00AD,0x00AE,0x0407,
    0x00B0,0x00B1,0x0406,0x0456,0x0491,0x00B5,0x00B6,0x00B7,
    0x0451,0x2116,0x0454,0x00BB,0x0458,0x0405,0x0455,0x0457,
    0x0410,0x0411,0x0412,0x0413,0x0414,0x0415,0x0416,0x0417,
    0x0418,0x0419,0x041A,0x041B,0x041C,0x041D,0x041E,0x041F,
    0x0420,0x0421,0x0422,0x0423,0x0424,0x0425,0x0426,0x0427,
    0x0428,0x0429,0x042A,0x042B,0x042C,0x042D,0x042E,0x042F,
    0x0430,0x0431,0x0432,0x0433,0x0434,0x0435,0x0436,0x0437,
    0x0438,0x0439,0x043A,0x043B,0x043C,0x043D,0x043E,0x043F,
    0x0440,0x0441,0x0442,0x0443,0x0444,0x0445,0x0446,0x0447,
    0x0448,0x0449,0x044A,0x044B,0x044C,0x044D,0x044E,0x044F
};

static const unsigned short CP866[128] = {
    0x0410,0x0411,0x0412,0x0413,0x0414,0x0415,0x0416,0x0417,
    0x0418,0x0419,0x041A,0x041B,0x041C,0x041D,0x041E,0x041F,
    0x0420,0x0421,0x0422,0x0423,0x0424,0x0425,0x0426,0x0427,
    0x0428,0x0429,0x042A,0x042B,0x042C,0x042D,0x042E,0x042F,
    0x0430,0x0431,0x0432,0x0433,0x0434,0x0435,0x0436,0x0437,
    0x0438,0x0439,0x043A,0x043B,0x043C,0x043D,0x043E,0x043F,
    0x2591,0x2592,0x2593,0x2502,0x2524,0x2561,0x2562,0x2556,
    0x2555,0x2563,0x2551,0x2557,0x255D,0x255C,0x255B,0x2510,
    0x2514,0x2534,0x252C,0x251C,0x2500,0x253C,0x255E,0x255F,
    0x255A,0x2554,0x2569,0x2566,0x2560,0x2550,0x256C,0x2567,
    0x2568,0x2564,0x2565,0x2559,0x2558,0x2552,0x2553,0x256B,
    0x256A,0x2518,0x250C,0x2588,0x2584,0x258C,0x2590,0x2580,
    0x0440,0x0441,0x0442,0x0443,0x0444,0x0445,0x0446,0x0447,
    0x0448,0x0449,0x044A,0x044B,0x044C,0x044D,0x044E,0x044F,
    0x0401,0x0451,0x0404,0x0454,0x0407,0x0457,0x040E,0x045E,
    0x00B0,0x2219,0x00B7,0x221A,0x2116,0x00A4,0x25A0,0x00A0
};

static const unsigned short KOI8R[128] = {
    0x2500,0x2502,0x250C,0x2510,0x2514,0x2518,0x251C,0x2524,
    0x252C,0x2534,0x253C,0x2580,0x2584,0x2588,0x258C,0x2590,
    0x2591,0x2592,0x2593,0x2320,0x25A0,0x2219,0x221A,0x2248,
    0x2264,0x2265,0x00A0,0x2321,0x00B0,0x00B2,0x00B7,0x00F7,
    0x2550,0x2551,0x2552,0x0451,0x2553,0x2554,0x2555,0x2556,
    0x2557,0x2558,0x2559,0x255A,0x255B,0x255C,0x255D,0x255E,
    0x255F,0x2560,0x2561,0x0401,0x2562,0x2563,0x2564,0x2565,
    0x2566,0x2567,0x2568,0x2569,0x256A,0x256B,0x256C,0x00A9,
    0x044E,0x0430,0x0431,0x0446,0x0434,0x0435,0x0444,0x0433,
    0x0445,0x0438,0x0439,0x043A,0x043B,0x043C,0x043D,0x043E,
    0x043F,0x044F,0x0440,0x0441,0x0442,0x0443,0x0436,0x0432,
    0x044C,0x044B,0x0437,0x0448,0x044D,0x0449,0x0447,0x044A,
    0x042E,0x0410,0x0411,0x0426,0x0414,0x0415,0x0424,0x0413,
    0x0425,0x0418,0x0419,0x041A,0x041B,0x041C,0x041D,0x041E,
    0x041F,0x042F,0x0420,0x0421,0x0422,0x0423,0x0416,0x0412,
    0x042C,0x042B,0x0417,0x0428,0x042D,0x0429,0x0427,0x042A
};

const char *textenc_name(TextEnc e)
{
    switch (e) {
        case TEXTENC_UTF8:    return "UTF-8";
        case TEXTENC_UTF16LE: return "UTF-16LE";
        case TEXTENC_UTF16BE: return "UTF-16BE";
        case TEXTENC_CP1251:  return "Windows-1251";
        case TEXTENC_CP866:   return "CP866";
        case TEXTENC_KOI8R:   return "KOI8-R";
        default:              return "не определена";
    }
}

TextEnc textenc_parse(const char *name)
{
    if (!name || !name[0]) return TEXTENC_UNKNOWN;
    if (!strcasecmp(name, "auto") || !strcasecmp(name, "авто")) return TEXTENC_UNKNOWN;
    if (!strcasecmp(name, "utf-8") || !strcasecmp(name, "utf8"))  return TEXTENC_UTF8;
    if (!strcasecmp(name, "utf-16le") || !strcasecmp(name, "utf16le")) return TEXTENC_UTF16LE;
    if (!strcasecmp(name, "utf-16be") || !strcasecmp(name, "utf16be")) return TEXTENC_UTF16BE;
    if (!strcasecmp(name, "cp1251") || !strcasecmp(name, "windows-1251") ||
        !strcasecmp(name, "win1251") || !strcasecmp(name, "1251")) return TEXTENC_CP1251;
    if (!strcasecmp(name, "cp866") || !strcasecmp(name, "ibm866") ||
        !strcasecmp(name, "866")) return TEXTENC_CP866;
    if (!strcasecmp(name, "koi8-r") || !strcasecmp(name, "koi8r") ||
        !strcasecmp(name, "koi8")) return TEXTENC_KOI8R;
    return TEXTENC_UNKNOWN;
}

size_t textenc_bom_len(const char *data, size_t len)
{
    const unsigned char *p = (const unsigned char *)data;
    if (len >= 3 && p[0] == 0xEF && p[1] == 0xBB && p[2] == 0xBF) return 3;
    if (len >= 2 && p[0] == 0xFF && p[1] == 0xFE) return 2;
    if (len >= 2 && p[0] == 0xFE && p[1] == 0xFF) return 2;
    return 0;
}

bool textenc_is_utf8(const char *data, size_t len)
{
    const unsigned char *p = (const unsigned char *)data;
    for (size_t i = 0; i < len; ) {
        unsigned char c = p[i];
        if (c < 0x80) { i++; continue; }

        int extra;
        unsigned int cp;
        if ((c & 0xE0) == 0xC0)      { extra = 1; cp = c & 0x1Fu; }
        else if ((c & 0xF0) == 0xE0) { extra = 2; cp = c & 0x0Fu; }
        else if ((c & 0xF8) == 0xF0) { extra = 3; cp = c & 0x07u; }
        else return false;                       /* одиночный байт продолжения */

        if (i + (size_t)extra >= len) return false;   /* оборванная последовательность */
        for (int k = 1; k <= extra; k++) {
            unsigned char cc = p[i + (size_t)k];
            if ((cc & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (cc & 0x3Fu);
        }
        /* Избыточно длинные записи и суррогаты — признак не-UTF-8: настоящий
         * UTF-8 их не содержит, а однобайтовый текст такие сочетания даёт. */
        if ((extra == 1 && cp < 0x80) || (extra == 2 && cp < 0x800) ||
            (extra == 3 && cp < 0x10000) || cp > 0x10FFFF ||
            (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += (size_t)extra + 1;
    }
    return true;
}

/* Насколько правдоподобен русский текст, если считать данные этой кодировкой.
 * Считаем долю байтов, дающих кириллицу, и штрафуем за псевдографику: в
 * выгрузках её не бывает, а при неверно угаданной кодировке её появляется
 * много — именно этим CP866 отличается от Windows-1251 на одних и тех же
 * данных. */
static long enc_score(const unsigned char *p, size_t len, const unsigned short *tbl)
{
    long score = 0;
    for (size_t i = 0; i < len; i++) {
        if (p[i] < 0x80) continue;
        unsigned short u = tbl[p[i] - 0x80];
        if (u == 0)                       score -= 4;   /* байт не определён */
        else if (u >= 0x0410 && u <= 0x044F) score += 3;   /* русские буквы */
        else if (u == 0x0401 || u == 0x0451) score += 3;   /* Ё, ё */
        else if (u >= 0x2500 && u <= 0x25FF) score -= 3;   /* псевдографика */
        else                              score -= 1;
    }
    return score;
}

TextEnc textenc_detect(const char *data, size_t len)
{
    const unsigned char *p = (const unsigned char *)data;
    if (len >= 3 && p[0] == 0xEF && p[1] == 0xBB && p[2] == 0xBF) return TEXTENC_UTF8;
    if (len >= 2 && p[0] == 0xFF && p[1] == 0xFE) return TEXTENC_UTF16LE;
    if (len >= 2 && p[0] == 0xFE && p[1] == 0xFF) return TEXTENC_UTF16BE;

    /* Смотрим на начало файла — этого достаточно, а гонять мегабайты незачем. */
    size_t probe = len < 65536 ? len : 65536;

    bool has_high = false;
    for (size_t i = 0; i < probe; i++) if (p[i] >= 0x80) { has_high = true; break; }
    if (!has_high) return TEXTENC_UTF8;          /* чистый ASCII — он же UTF-8 */

    if (textenc_is_utf8(data, probe)) return TEXTENC_UTF8;

    long s1251 = enc_score(p, probe, CP1251);
    long s866  = enc_score(p, probe, CP866);
    long skoi  = enc_score(p, probe, KOI8R);

    /* При равенстве выбираем Windows-1251: в выгрузках, которые к нам
     * попадают, она встречается несравнимо чаще прочих. */
    if (s1251 >= s866 && s1251 >= skoi) return s1251 > 0 ? TEXTENC_CP1251 : TEXTENC_UNKNOWN;
    if (s866  >= skoi)                  return s866  > 0 ? TEXTENC_CP866  : TEXTENC_UNKNOWN;
    return skoi > 0 ? TEXTENC_KOI8R : TEXTENC_UNKNOWN;
}

/* Записывает код символа в UTF-8. Возвращает число записанных байтов. */
static size_t put_utf8(char *out, unsigned int cp)
{
    unsigned char *o = (unsigned char *)out;
    if (cp < 0x80)    { o[0] = (unsigned char)cp; return 1; }
    if (cp < 0x800)   { o[0] = (unsigned char)(0xC0 | (cp >> 6));
                        o[1] = (unsigned char)(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) { o[0] = (unsigned char)(0xE0 | (cp >> 12));
                        o[1] = (unsigned char)(0x80 | ((cp >> 6) & 0x3F));
                        o[2] = (unsigned char)(0x80 | (cp & 0x3F)); return 3; }
    o[0] = (unsigned char)(0xF0 | (cp >> 18));
    o[1] = (unsigned char)(0x80 | ((cp >> 12) & 0x3F));
    o[2] = (unsigned char)(0x80 | ((cp >> 6) & 0x3F));
    o[3] = (unsigned char)(0x80 | (cp & 0x3F));
    return 4;
}

static char *from_single_byte(const unsigned char *p, size_t len,
                              const unsigned short *tbl, size_t *out_len)
{
    /* Худший случай — 3 байта UTF-8 на каждый исходный байт. */
    char *out = malloc(len * 3 + 1);
    if (!out) return NULL;
    size_t o = 0;
    for (size_t i = 0; i < len; i++) {
        if (p[i] < 0x80) { out[o++] = (char)p[i]; continue; }
        unsigned short u = tbl[p[i] - 0x80];
        o += put_utf8(out + o, u ? u : 0xFFFD);   /* неопределённый байт → «замена» */
    }
    out[o] = 0;
    if (out_len) *out_len = o;
    return out;
}

static char *from_utf16(const unsigned char *p, size_t len, bool little, size_t *out_len)
{
    char *out = malloc(len * 2 + 1);             /* 2 байта UTF-16 → до 4 UTF-8 */
    if (!out) return NULL;
    size_t o = 0;
    for (size_t i = 0; i + 1 < len; i += 2) {
        unsigned int u = little ? (unsigned int)(p[i] | (p[i + 1] << 8))
                                : (unsigned int)((p[i] << 8) | p[i + 1]);
        /* Суррогатная пара — символ вне основной плоскости. */
        if (u >= 0xD800 && u <= 0xDBFF && i + 3 < len) {
            unsigned int lo = little ? (unsigned int)(p[i + 2] | (p[i + 3] << 8))
                                     : (unsigned int)((p[i + 2] << 8) | p[i + 3]);
            if (lo >= 0xDC00 && lo <= 0xDFFF) {
                u = 0x10000 + ((u - 0xD800) << 10) + (lo - 0xDC00);
                i += 2;
            }
        }
        o += put_utf8(out + o, u);
    }
    out[o] = 0;
    if (out_len) *out_len = o;
    return out;
}

char *textenc_to_utf8(const char *data, size_t len, TextEnc enc,
                      size_t *out_len, TextEnc *used)
{
    if (!data) return NULL;

    size_t bom = textenc_bom_len(data, len);
    if (enc == TEXTENC_UNKNOWN) enc = textenc_detect(data, len);
    if (used) *used = enc;

    const unsigned char *p = (const unsigned char *)data + bom;
    size_t n = len - bom;

    switch (enc) {
        case TEXTENC_CP1251: return from_single_byte(p, n, CP1251, out_len);
        case TEXTENC_CP866:  return from_single_byte(p, n, CP866,  out_len);
        case TEXTENC_KOI8R:  return from_single_byte(p, n, KOI8R,  out_len);
        case TEXTENC_UTF16LE: return from_utf16(p, n, true,  out_len);
        case TEXTENC_UTF16BE: return from_utf16(p, n, false, out_len);
        default: break;
    }

    /* UTF-8 и нераспознанное — копируем как есть, сняв метку порядка байтов.
     * Догадка на нераспознанных данных испортила бы их сильнее, чем
     * бездействие: неверная таблица переводит каждый байт в чужую букву. */
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = 0;
    if (out_len) *out_len = n;
    return out;
}
